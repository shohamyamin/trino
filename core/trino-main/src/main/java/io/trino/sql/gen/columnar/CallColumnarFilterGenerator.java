/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.project.InputChannels;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.Binding;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.DEFAULT_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.sql.gen.BytecodeUtils.generateToString;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.BytecodeUtils.loadBindingHandle;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.createClassInstanceDirect;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.declareBlockVariables;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockMayHaveNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockPositionNotNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateGetInputChannels;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.updateOutputPositions;
import static io.trino.util.CompilerUtils.isClassDumpEnabled;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CallColumnarFilterGenerator
{
    private final ResolvedFunction function;
    private final List<Expression> arguments;
    private final Map<Symbol, Integer> layout;
    private final FunctionManager functionManager;

    public CallColumnarFilterGenerator(ResolvedFunction function, List<Expression> arguments, Map<Symbol, Integer> layout, FunctionManager functionManager)
    {
        arguments.forEach(expression -> {
            if (!(expression instanceof Reference) && !(expression instanceof Constant)) {
                throw new UnsupportedOperationException("Call expression with unsupported argument: " + expression);
            }
            if (expression instanceof Constant constant) {
                if (constant.value() == null) {
                    throw new UnsupportedOperationException("Call expressions with null constant are not supported");
                }
            }
        });
        function.signature().getArgumentTypes().forEach(argumentType -> {
            if (argumentType instanceof FunctionType) {
                throw new UnsupportedOperationException("Functions with lambda arguments are not supported");
            }
        });
        this.function = requireNonNull(function, "function is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
        this.layout = requireNonNull(layout, "layout is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    /**
     * Returns the key of the class generated for this call. The class does not depend on the
     * values of the constant arguments, which are passed to its constructor (see
     * {@link #constantValues}), so all calls of the same function on the same input channels
     * and constant types share one class, and one JIT compilation of it. This matters most
     * for dynamic filters, which are compiled again with new range bounds on every update.
     */
    public CallFilterShape shape()
    {
        ImmutableList.Builder<Object> argumentShapes = ImmutableList.builder();
        for (Expression argument : arguments) {
            switch (argument) {
                case Reference reference -> argumentShapes.add(layout.get(Symbol.from(reference)));
                // boolean constants are compiled to bytecode constants
                case Constant constant when constant.type().getJavaType() == boolean.class -> argumentShapes.add(new BooleanConstantShape((Boolean) constant.value()));
                case Constant constant -> argumentShapes.add(new ConstantShape(constant.type()));
                default -> throw new UnsupportedOperationException("Unsupported argument expression: " + argument);
            }
        }
        return new CallFilterShape(function, argumentShapes.build());
    }

    /**
     * Values of the constant arguments, other than boolean, in argument order, as expected by
     * the constructor of the generated class.
     */
    public static Object[] constantValues(List<Expression> arguments)
    {
        return arguments.stream()
                .filter(Constant.class::isInstance)
                .map(Constant.class::cast)
                .filter(constant -> constant.type().getJavaType() != boolean.class)
                .map(Constant::value)
                .toArray();
    }

    public record CallFilterShape(ResolvedFunction function, List<Object> argumentShapes)
    {
        public CallFilterShape
        {
            requireNonNull(function, "function is null");
            argumentShapes = ImmutableList.copyOf(argumentShapes);
        }
    }

    private record ConstantShape(Type type) {}

    private record BooleanConstantShape(boolean value) {}

    public Class<? extends ColumnarFilter> generateColumnarFilter()
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = defineFilterClass(callSiteBinder);
        return createClassInstanceDirect(callSiteBinder, classDefinition);
    }

    private ClassDefinition defineFilterClass(CallSiteBinder callSiteBinder)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(ColumnarFilter.class.getSimpleName() + function.signature().getName(), Optional.empty()),
                type(Object.class),
                type(ColumnarFilter.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        FieldDefinition inputChannelsField = generateGetInputChannels(classDefinition);

        // one field for each constant argument, other than boolean, set by the constructor
        Map<Integer, FieldDefinition> constantFields = new HashMap<>();
        for (int argument = 0; argument < arguments.size(); argument++) {
            if (arguments.get(argument) instanceof Constant constant && constant.type().getJavaType() != boolean.class) {
                Class<?> fieldType = callSiteBinder.getAccessibleType(constant.type().getJavaType());
                constantFields.put(argument, classDefinition.declareField(a(PRIVATE, FINAL), "constant_" + argument, fieldType));
            }
        }

        generateFilterRangeMethod(classDefinition, callSiteBinder, cachedInstanceBinder, constantFields);
        generateFilterListMethod(classDefinition, callSiteBinder, cachedInstanceBinder, constantFields);

        generateConstructor(classDefinition, cachedInstanceBinder, inputChannelsField, constantFields);

        String description = ColumnarFilter.class.getSimpleName() + "{" + function.signature() + ", arguments=" + shape().argumentShapes() + "}";
        generateToString(classDefinition, callSiteBinder.bind(description, Object.class));
        return classDefinition;
    }

    // loads the constant fields into local variables once per batch, ahead of the loops
    private static void declareConstantVariables(Map<Integer, FieldDefinition> constantFields, Scope scope, BytecodeBlock body)
    {
        constantFields.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> scope.declareVariable("constant_" + entry.getKey(), body, scope.getThis().getField(entry.getValue())));
    }

    private void generateFilterRangeMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, Map<Integer, FieldDefinition> constantFields)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", SourcePage.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsRange",
                type(int.class),
                ImmutableList.of(session, outputPositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(arguments, layout, page, scope, body);
        declareConstantVariables(constantFields, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        FunctionNullability functionNullability = function.functionNullability();
        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(arguments, layout, functionNullability.getArgumentNullable(), scope));
        body.append(ifStatement);
        Function<MethodHandle, BytecodeNode> instance = instanceFactory -> scope.getThis().getField(cachedInstanceBinder.getCachedInstance(instanceFactory));

        /* if (block_0.mayHaveNull() || block_1.mayHaveNull()...) {
         *     for (position = offset; position < offset + size; position++) {
         *         if (!block_0.isNull(position) && !block_1.isNull(position)...) {
         *             boolean result = call_function(position, block_0, block_1, ...);
         *             outputPositions[outputPositionsCount] = position;
         *             outputPositionsCount += result ? 1 : 0;
         *         }
         *     }
         * }
         */
        ifStatement.ifTrue(new ForLoop("nullable range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(new IfStatement()
                        .condition(generateBlockPositionNotNull(arguments, layout, functionNullability.getArgumentNullable(), scope, position))
                        .ifTrue(new BytecodeBlock()
                                .append(generateFullInvocation(functionManager, instance, callSiteBinder, function, arguments, layout, scope, position)
                                        .putVariable(result))
                                .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount)))));

        /* for (position = offset; position < offset + size; position++) {
         *     boolean result = call_function(position, block_0, block_1, ...);
         *     outputPositions[outputPositionsCount] = position;
         *     outputPositionsCount += result ? 1 : 0;
         * }
         */
        ifStatement.ifFalse(new ForLoop("nullable function range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(new BytecodeBlock()
                        .append(generateFullInvocation(functionManager, instance, callSiteBinder, function, arguments, layout, scope, position)
                                .putVariable(result))
                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))));

        body.append(outputPositionsCount.ret());
    }

    private void generateFilterListMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, Map<Integer, FieldDefinition> constantFields)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter activePositions = arg("activePositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", SourcePage.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsList",
                type(int.class),
                ImmutableList.of(session, outputPositions, activePositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(arguments, layout, page, scope, body);
        declareConstantVariables(constantFields, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable index = scope.declareVariable(int.class, "index");
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        FunctionNullability functionNullability = function.functionNullability();
        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(arguments, layout, functionNullability.getArgumentNullable(), scope));
        body.append(ifStatement);
        Function<MethodHandle, BytecodeNode> instance = instanceFactory -> scope.getThis().getField(cachedInstanceBinder.getCachedInstance(instanceFactory));

        /* if (block_0.mayHaveNull() || block_1.mayHaveNull()...) {
         *     for (int index = offset; index < offset + size; index++) {
         *         int position = activePositions[index];
         *         if (!block_0.isNull(position) && !block_1.isNull(position)...) {
         *             boolean result = call_function(position, block_0, block_1, ...);
         *             outputPositions[outputPositionsCount] = position;
         *             outputPositionsCount += result ? 1 : 0;
         *         }
         *     }
         * }
         */
        ifStatement.ifTrue(new ForLoop("nullable positions loop")
                .initialize(index.set(offset))
                .condition(lessThan(index, add(offset, size)))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(index)))
                        .append(new IfStatement()
                                .condition(generateBlockPositionNotNull(arguments, layout, functionNullability.getArgumentNullable(), scope, position))
                                .ifTrue(new BytecodeBlock()
                                        .append(generateFullInvocation(functionManager, instance, callSiteBinder, function, arguments, layout, scope, position)
                                                .putVariable(result))
                                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))))));

        /* for (int index = offset; index < offset + size; index++) {
         *     int position = activePositions[index];
         *     boolean result = call_function(position, block_0, block_1, ...);
         *     outputPositions[outputPositionsCount] = position;
         *     outputPositionsCount += result ? 1 : 0;
         * }
         */
        ifStatement.ifFalse(new ForLoop("non-nullable positions loop")
                .initialize(index.set(offset))
                .condition(lessThan(index, add(offset, size)))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(index)))
                        .append(generateFullInvocation(functionManager, instance, callSiteBinder, function, arguments, layout, scope, position)
                                .putVariable(result))
                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))));

        body.append(outputPositionsCount.ret());
    }

    private static BytecodeBlock generateFullInvocation(
            FunctionManager functionManager,
            Function<MethodHandle, BytecodeNode> instanceFactory,
            CallSiteBinder binder,
            ResolvedFunction function,
            List<Expression> arguments,
            Map<Symbol, Integer> layout,
            Scope scope,
            BytecodeExpression position)
    {
        String functionName = function.signature().getName().functionName();
        BytecodeBlock block = new BytecodeBlock()
                .setDescription("invoke " + functionName);

        ScalarFunctionImplementation implementation = getScalarFunctionImplementation(functionManager, function, arguments);

        Binding binding = binder.bind(implementation.getMethodHandle());

        Optional<BytecodeNode> instance = implementation.getInstanceFactory()
                .map(instanceFactory);

        MethodType methodType = binding.getType();
        boolean classDataBinding = binding.getKind() == Binding.Kind.HANDLE;
        if (classDataBinding) {
            block.append(loadBindingHandle(binding));
        }

        // Index of current parameter in the MethodHandle
        int currentParameterIndex = 0;
        boolean instanceIsBound = false;
        while (currentParameterIndex < methodType.parameterArray().length) {
            Class<?> type = methodType.parameterArray()[currentParameterIndex];
            if (instance.isPresent() && !instanceIsBound) {
                checkState(type.equals(binder.getAccessibleType(implementation.getInstanceFactory().get().type().returnType())), "Mismatched type for instance parameter");
                block.append(instance.get());
                instanceIsBound = true;
            }
            else if (type == ConnectorSession.class) {
                block.append(scope.getVariable("session"));
            }
            currentParameterIndex++;
        }
        for (int argument = 0; argument < arguments.size(); argument++) {
            Expression argumentExpression = arguments.get(argument);
            if (argumentExpression instanceof Reference reference) {
                Integer channel = layout.get(Symbol.from(reference));
                checkState(channel != null, "Reference not in layout: %s", reference.name());
                block.append(generateInputReference(scope.getVariable("block_" + channel), position));
            }
            else if (argumentExpression instanceof Constant constant) {
                block.append(generateConstant(scope, argument, constant));
            }
            else {
                throw new UnsupportedOperationException(format("Unsupported argument expression: %s", argumentExpression));
            }
        }
        if (classDataBinding) {
            block.invokeVirtual(MethodHandle.class, "invokeExact", methodType.returnType(), methodType.parameterArray());
        }
        else {
            block.append(invoke(binding, functionName));
        }
        return block;
    }

    private static ScalarFunctionImplementation getScalarFunctionImplementation(FunctionManager functionManager, ResolvedFunction resolvedFunction, List<Expression> arguments)
    {
        ImmutableList.Builder<InvocationConvention.InvocationArgumentConvention> builder = ImmutableList.builderWithExpectedSize(arguments.size());
        for (Expression argumentExpression : arguments) {
            if (argumentExpression instanceof Reference) {
                builder.add(BLOCK_POSITION);
            }
            else if (argumentExpression instanceof Constant) {
                builder.add(NEVER_NULL);
            }
            else {
                throw new UnsupportedOperationException(format("Unsupported argument expression: %s", argumentExpression));
            }
        }

        InvocationConvention invocationConvention = new InvocationConvention(
                builder.build(),
                resolvedFunction.functionNullability().isReturnNullable() ? DEFAULT_ON_NULL : FAIL_ON_NULL,
                true,
                true);
        return functionManager.getScalarFunctionImplementation(resolvedFunction, invocationConvention);
    }

    private static BytecodeNode generateInputReference(BytecodeExpression block, BytecodeExpression position)
    {
        BytecodeBlock blockAndPosition = new BytecodeBlock();
        blockAndPosition.append(block);
        blockAndPosition.append(position);
        return blockAndPosition;
    }

    private static BytecodeNode generateConstant(Scope scope, int argument, Constant constant)
    {
        // boolean constants stay as bytecode constants: the two values change control flow
        // shapes anyway and are part of the class key
        if (constant.type().getJavaType() == boolean.class) {
            BytecodeBlock block = new BytecodeBlock();
            if (isClassDumpEnabled()) {
                block.comment("constant " + constant.type().getTypeDescriptor());
            }
            return block.append(loadBoolean((Boolean) constant.value()));
        }

        // All other constants, including primitives, are set by the constructor, so the
        // generated class depends only on the filter structure and is shared by filters
        // that differ only in constant values
        return new BytecodeBlock()
                .setDescription("constant " + constant.type())
                .append(scope.getVariable("constant_" + argument));
    }

    private static void generateConstructor(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder, FieldDefinition inputChannelsField, Map<Integer, FieldDefinition> constantFields)
    {
        Parameter inputChannelsParam = arg("inputChannels", InputChannels.class);
        Parameter constantsParam = arg("constants", Object[].class);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), inputChannelsParam, constantsParam);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(inputChannelsField, inputChannelsParam));
        List<FieldDefinition> fields = constantFields.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .collect(toImmutableList());
        for (int constant = 0; constant < fields.size(); constant++) {
            FieldDefinition field = fields.get(constant);
            body.append(thisVariable.setField(field, constantsParam.getElement(constant).cast(field.getType())));
        }
        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
    }

    private static final class CachedInstanceBinder
    {
        private final ClassDefinition classDefinition;
        private final CallSiteBinder callSiteBinder;
        private Optional<FieldDefinition> field = Optional.empty();
        private Optional<MethodHandle> method = Optional.empty();

        public CachedInstanceBinder(ClassDefinition classDefinition, CallSiteBinder callSiteBinder)
        {
            this.classDefinition = requireNonNull(classDefinition, "classDefinition is null");
            this.callSiteBinder = requireNonNull(callSiteBinder, "callSiteBinder is null");
        }

        public FieldDefinition getCachedInstance(MethodHandle methodHandle)
        {
            if (field.isEmpty()) {
                field = Optional.of(classDefinition.declareField(a(PRIVATE, FINAL), "__cachedInstance", callSiteBinder.getAccessibleType(methodHandle.type().returnType())));
                method = Optional.of(methodHandle);
            }
            return field.get();
        }

        public void generateInitializations(Variable thisVariable, BytecodeBlock block)
        {
            if (field.isEmpty()) {
                return;
            }
            Binding binding = callSiteBinder.bind(method.orElseThrow());
            block.append(thisVariable.setField(field.get(), invoke(binding, "instance")));
        }
    }
}

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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.expression.Constant;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;

public class TestPushAggregationIntoTableScan
        extends BaseRuleTest
{
    private PushAggregationIntoTableScan rule;
    private TableHandle tableHandle;

    @Override
    protected Optional<PlanTester> createPlanTester()
    {
        Session defaultSession = TestingSession.testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema("tiny")
                .build();

        PlanTester planTester = PlanTester.create(defaultSession);
        planTester.createCatalog(
                TEST_CATALOG_NAME,
                MockConnectorFactory.builder()
                        // the connector knows the results of global aggregations
                        .withApplyAggregation((_, handle, aggregates, _, groupingSets) -> {
                            if (!groupingSets.equals(List.of(List.of()))) {
                                return Optional.empty();
                            }
                            return Optional.of(new AggregationApplicationResult<>(
                                    handle,
                                    aggregates.stream()
                                            .map(aggregate -> new Constant(aggregate.getFunctionName().equals("count") ? 25L : null, aggregate.getOutputType()))
                                            .collect(toImmutableList()),
                                    List.of(),
                                    Map.of(),
                                    false));
                        })
                        .build(),
                Map.of());
        return Optional.of(planTester);
    }

    @BeforeAll
    public void init()
    {
        rule = new PushAggregationIntoTableScan(tester().getPlannerContext());
        tableHandle = tester().getCurrentCatalogTableHandle("mock_schema", "mock_nation");
    }

    @Test
    public void testGlobalAggregationWithConstantResults()
    {
        tester().assertThat(rule)
                .on(p -> {
                    Symbol regionkey = p.symbol("regionkey", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .step(SINGLE)
                            .addAggregation(p.symbol("count", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                            .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(regionkey.toSymbolReference())), ImmutableList.of(BIGINT))
                            .source(p.tableScan(
                                    tableHandle,
                                    ImmutableList.of(regionkey),
                                    ImmutableMap.of(regionkey, new MockConnectorColumnHandle("regionkey", BIGINT)))));
                })
                .matches(values(
                        ImmutableList.of("count", "max"),
                        ImmutableList.of(ImmutableList.of(
                                new io.trino.sql.ir.Constant(BIGINT, 25L),
                                new io.trino.sql.ir.Constant(BIGINT, null)))));
    }
}

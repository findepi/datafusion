// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::analyzer::{Analyzer, AnalyzerRule};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{table_scan, LogicalPlan};
use std::sync::Arc;

/// some tests share a common table
pub fn test_table_scan() -> datafusion_common::Result<LogicalPlan> {
    test_table_scan_with_name("test")
}

/// some tests share a common table with different names
pub fn test_table_scan_with_name(name: &str) -> datafusion_common::Result<LogicalPlan> {
    let schema = Schema::new(test_table_scan_fields());
    table_scan(Some(name), &schema, None)?.build()
}

pub fn test_table_scan_fields() -> Vec<Field> {
    vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::UInt32, false),
        Field::new("c", DataType::UInt32, false),
    ]
}

pub fn assert_analyzed_plan_eq(
    rule: Arc<dyn AnalyzerRule + Send + Sync>,
    plan: LogicalPlan,
    expected: &str,
) -> datafusion_common::Result<()> {
    let options = ConfigOptions::default();
    assert_analyzed_plan_with_config_eq(options, rule, plan, expected)?;

    Ok(())
}

pub fn assert_analyzed_plan_with_config_eq(
    options: ConfigOptions,
    rule: Arc<dyn AnalyzerRule + Send + Sync>,
    plan: LogicalPlan,
    expected: &str,
) -> datafusion_common::Result<()> {
    let analyzed_plan =
        Analyzer::with_rules(vec![rule]).execute_and_check(plan, &options, |_, _| {})?;
    let formatted_plan = format!("{analyzed_plan}");
    assert_eq!(formatted_plan, expected);

    Ok(())
}

pub fn assert_analyzed_plan_eq_display_indent(
    rule: Arc<dyn AnalyzerRule + Send + Sync>,
    plan: LogicalPlan,
    expected: &str,
) -> datafusion_common::Result<()> {
    let options = ConfigOptions::default();
    let analyzed_plan =
        Analyzer::with_rules(vec![rule]).execute_and_check(plan, &options, |_, _| {})?;
    let formatted_plan = analyzed_plan.display_indent_schema().to_string();
    assert_eq!(formatted_plan, expected);

    Ok(())
}

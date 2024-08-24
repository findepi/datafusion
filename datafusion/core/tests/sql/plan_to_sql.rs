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

use datafusion_sql::unparser::Unparser;
use datafusion_common::Result;
use super::{SessionConfig, SessionContext};
use assertor::{assert_that, EqualityAssertion};

#[tokio::test]
async fn show_tables_plan() -> Result<()> {
    assert_statement_round_trips("SHOW TABLES").await
}

#[tokio::test]
async fn show_columns_plan() -> Result<()> {
    assert_statement_round_trips("SHOW COLUMNS FROM person").await
}

async fn assert_statement_round_trips(sql: &str) -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    let data_frame = ctx.sql(sql).await?;
    let logical_plan = data_frame.logical_plan();

    let unparser = Unparser::default();
    let formatted = format!("{}",  unparser.plan_to_sql(logical_plan)?);
    assert_that!(formatted).is_equal_to(sql.to_string());

    Ok(())
}
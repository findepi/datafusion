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

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expr {
    Integer(i64),
    Variable(String),
    ArithmeticBinary(Box<Expr>, ArithmeticOp, Box<Expr>),
    ComparisonBinary(Box<Expr>, ComparisonOp, Box<Expr>),
    Call(Function, Vec<Expr>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ArithmeticOp {
    Add,
    Subtract,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ComparisonOp {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
    NotEqual,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Function {
    Min,
    Max,
}

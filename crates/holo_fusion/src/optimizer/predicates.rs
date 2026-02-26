use std::collections::BTreeMap;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};

#[derive(Debug, Clone)]
pub struct PredicateBound {
    pub value: ScalarValue,
    pub inclusive: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ColumnPredicate {
    pub equals: Vec<ScalarValue>,
    pub lower: Option<PredicateBound>,
    pub upper: Option<PredicateBound>,
    pub is_null: bool,
    pub is_not_null: bool,
}

impl ColumnPredicate {
    fn add_equality(&mut self, value: ScalarValue) {
        if !self.equals.iter().any(|existing| existing == &value) {
            self.equals.push(value);
        }
    }

    fn add_lower_bound(&mut self, value: ScalarValue, inclusive: bool) {
        self.lower = match self.lower.take() {
            None => Some(PredicateBound { value, inclusive }),
            Some(existing) => {
                if compare_literal_order(&existing.value, &value)
                    .map(|cmp| cmp.is_lt())
                    .unwrap_or(false)
                    || (existing.value == value && !inclusive && existing.inclusive)
                {
                    Some(PredicateBound { value, inclusive })
                } else {
                    Some(existing)
                }
            }
        };
    }

    fn add_upper_bound(&mut self, value: ScalarValue, inclusive: bool) {
        self.upper = match self.upper.take() {
            None => Some(PredicateBound { value, inclusive }),
            Some(existing) => {
                if compare_literal_order(&existing.value, &value)
                    .map(|cmp| cmp.is_gt())
                    .unwrap_or(false)
                    || (existing.value == value && !inclusive && existing.inclusive)
                {
                    Some(PredicateBound { value, inclusive })
                } else {
                    Some(existing)
                }
            }
        };
    }

    pub fn has_any_constraint(&self) -> bool {
        !self.equals.is_empty()
            || self.lower.is_some()
            || self.upper.is_some()
            || self.is_null
            || self.is_not_null
    }
}

#[derive(Debug, Clone, Default)]
pub struct PredicateSummary {
    pub by_column: BTreeMap<String, ColumnPredicate>,
    pub total_terms: usize,
    pub recognized_terms: usize,
    pub residual_terms: usize,
}

impl PredicateSummary {
    pub fn columns(&self) -> impl Iterator<Item = (&String, &ColumnPredicate)> {
        self.by_column.iter()
    }

    pub fn column(&self, name: &str) -> Option<&ColumnPredicate> {
        self.by_column.get(name)
    }

    pub fn residual_fraction(&self) -> f64 {
        if self.total_terms == 0 {
            return 0.0;
        }
        self.residual_terms as f64 / self.total_terms as f64
    }
}

pub fn extract_predicate_summary(filters: &[Expr]) -> PredicateSummary {
    let mut summary = PredicateSummary::default();
    for filter in filters {
        let mut terms = Vec::<&Expr>::new();
        collect_conjuncts(filter, &mut terms);
        for term in terms {
            summary.total_terms = summary.total_terms.saturating_add(1);
            if apply_term(term, &mut summary.by_column) {
                summary.recognized_terms = summary.recognized_terms.saturating_add(1);
            } else {
                summary.residual_terms = summary.residual_terms.saturating_add(1);
            }
        }
    }
    summary
}

fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_conjuncts(binary.left.as_ref(), out);
            collect_conjuncts(binary.right.as_ref(), out);
        }
        _ => out.push(expr),
    }
}

fn apply_term(expr: &Expr, by_column: &mut BTreeMap<String, ColumnPredicate>) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => {
            let parsed = match (
                extract_column_name(binary.left.as_ref()),
                extract_literal(binary.right.as_ref()),
            ) {
                (Some(column), Some(literal)) => {
                    Some((canonicalize_column_name(column), binary.op, literal))
                }
                _ => match (
                    extract_column_name(binary.right.as_ref()),
                    extract_literal(binary.left.as_ref()),
                ) {
                    (Some(column), Some(literal)) => Some((
                        canonicalize_column_name(column),
                        reverse_comparison(binary.op),
                        literal,
                    )),
                    _ => None,
                },
            };

            let Some((column, op, literal)) = parsed else {
                return false;
            };
            let pred = by_column.entry(column).or_default();
            match op {
                Operator::Eq => pred.add_equality(literal),
                Operator::Lt => pred.add_upper_bound(literal, false),
                Operator::LtEq => pred.add_upper_bound(literal, true),
                Operator::Gt => pred.add_lower_bound(literal, false),
                Operator::GtEq => pred.add_lower_bound(literal, true),
                _ => return false,
            }
            true
        }
        Expr::InList(inlist) => {
            if inlist.negated {
                return false;
            }
            let Some(column) = extract_column_name(inlist.expr.as_ref()) else {
                return false;
            };
            let pred = by_column
                .entry(canonicalize_column_name(column))
                .or_default();
            if inlist.list.is_empty() {
                return false;
            }
            for item in &inlist.list {
                let Some(literal) = extract_literal(item) else {
                    return false;
                };
                pred.add_equality(literal);
            }
            true
        }
        Expr::IsNull(expr) => {
            let Some(column) = extract_column_name(expr.as_ref()) else {
                return false;
            };
            let pred = by_column
                .entry(canonicalize_column_name(column))
                .or_default();
            pred.is_null = true;
            true
        }
        Expr::IsNotNull(expr) => {
            let Some(column) = extract_column_name(expr.as_ref()) else {
                return false;
            };
            let pred = by_column
                .entry(canonicalize_column_name(column))
                .or_default();
            pred.is_not_null = true;
            true
        }
        _ => false,
    }
}

fn canonicalize_column_name(column: &str) -> String {
    // DataFusion can preserve internal aliases such as:
    // - `Use event_day`
    // - `Use event_day@1`
    // - `sales_facts.Use event_day@1`
    // Normalize these to plain logical column names for index matching.
    let mut normalized = column.trim();
    normalized = strip_use_prefix(normalized);
    if let Some((_, tail)) = normalized.rsplit_once('.') {
        normalized = tail;
    }
    normalized = strip_wrapping_identifier_quotes(normalized);
    normalized = strip_use_prefix(normalized);
    normalized = strip_projection_slot_suffixes(normalized);
    normalized = strip_wrapping_identifier_quotes(normalized);
    normalized.to_ascii_lowercase()
}

fn strip_use_prefix(value: &str) -> &str {
    let mut current = value.trim();
    loop {
        let lowered = current.to_ascii_lowercase();
        if lowered.starts_with("use") {
            let bytes = lowered.as_bytes();
            if bytes.len() > 3 && bytes[3].is_ascii_whitespace() {
                let mut consumed = 3usize;
                while consumed < bytes.len() && bytes[consumed].is_ascii_whitespace() {
                    consumed += 1;
                }
                current = current[consumed..].trim_start();
                continue;
            }
        }
        return current;
    }
}

fn strip_projection_slot_suffixes(value: &str) -> &str {
    let mut current = value.trim_end();
    loop {
        let stripped = strip_projection_slot_suffix(current);
        if stripped.len() == current.len() {
            return current;
        }
        current = stripped;
    }
}

fn strip_projection_slot_suffix(value: &str) -> &str {
    let trimmed = value.trim_end();
    let bytes = trimmed.as_bytes();
    let mut idx = bytes.len();
    while idx > 0 && bytes[idx - 1].is_ascii_digit() {
        idx -= 1;
    }
    if idx == bytes.len() || idx == 0 || bytes[idx - 1] != b'@' {
        return trimmed;
    }
    trimmed[..idx - 1].trim_end()
}

fn strip_wrapping_identifier_quotes(value: &str) -> &str {
    let mut current = value.trim();
    loop {
        let bytes = current.as_bytes();
        if bytes.len() < 2 {
            return current;
        }
        let wrapped = matches!(
            (bytes[0], bytes[bytes.len() - 1]),
            (b'"', b'"') | (b'`', b'`') | (b'[', b']')
        );
        if !wrapped {
            return current;
        }
        current = current[1..bytes.len() - 1].trim();
    }
}

fn extract_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::Alias(alias) => extract_column_name(alias.expr.as_ref()),
        Expr::Cast(cast) => extract_column_name(cast.expr.as_ref()),
        Expr::TryCast(cast) => extract_column_name(cast.expr.as_ref()),
        _ => None,
    }
}

fn extract_literal(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Alias(alias) => extract_literal(alias.expr.as_ref()),
        Expr::Cast(cast) => extract_literal(cast.expr.as_ref()),
        Expr::TryCast(cast) => extract_literal(cast.expr.as_ref()),
        _ => None,
    }
}

fn reverse_comparison(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

fn compare_literal_order(left: &ScalarValue, right: &ScalarValue) -> Option<std::cmp::Ordering> {
    if let (Some(l), Some(r)) = (scalar_to_i128(left), scalar_to_i128(right)) {
        return Some(l.cmp(&r));
    }

    match (left, right) {
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r)))
        | (ScalarValue::LargeUtf8(Some(l)), ScalarValue::LargeUtf8(Some(r)))
        | (ScalarValue::Utf8(Some(l)), ScalarValue::LargeUtf8(Some(r)))
        | (ScalarValue::LargeUtf8(Some(l)), ScalarValue::Utf8(Some(r))) => Some(l.cmp(r)),
        (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => Some(l.cmp(r)),
        _ => None,
    }
}

fn scalar_to_i128(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(i128::from(*v)),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(i128::from(*v)),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(i128::from(*v)),
        ScalarValue::TimestampSecond(Some(v), _) => Some(i128::from(*v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    #[test]
    fn extracts_equality_and_inlist_predicates() {
        let filters = vec![col("status")
            .eq(lit("paid"))
            .and(col("event_day").in_list(vec![lit(1i64), lit(2i64)], false))];
        let summary = extract_predicate_summary(filters.as_slice());
        assert_eq!(summary.total_terms, 2);
        assert_eq!(summary.recognized_terms, 2);
        assert_eq!(summary.residual_terms, 0);

        let status = summary.column("status").expect("status predicate");
        assert_eq!(status.equals.len(), 1);
        let day = summary.column("event_day").expect("event_day predicate");
        assert_eq!(day.equals.len(), 2);
    }

    #[test]
    fn extracts_inlist_predicates_from_aliased_column_expr() {
        let filters = vec![col("event_day")
            .alias("use event_day")
            .in_list(vec![lit(20i64), lit(21i64), lit(22i64), lit(23i64)], false)];
        let summary = extract_predicate_summary(filters.as_slice());
        let day = summary.column("event_day").expect("event_day predicate");
        assert_eq!(day.equals.len(), 4);
        assert_eq!(summary.recognized_terms, 1);
        assert_eq!(summary.residual_terms, 0);
    }

    #[test]
    fn extracts_inlist_predicates_from_use_prefixed_column_name() {
        let filters = vec![col("Use event_day")
            .in_list(vec![lit(20i64), lit(21i64), lit(22i64), lit(23i64)], false)];
        let summary = extract_predicate_summary(filters.as_slice());
        let day = summary.column("event_day").expect("event_day predicate");
        assert_eq!(day.equals.len(), 4);
        assert_eq!(summary.recognized_terms, 1);
        assert_eq!(summary.residual_terms, 0);
    }

    #[test]
    fn canonicalize_column_name_strips_use_slot_suffix_and_qualifier() {
        assert_eq!(canonicalize_column_name("Use event_day@1"), "event_day");
        assert_eq!(
            canonicalize_column_name("sales_facts.Use event_day@1"),
            "event_day"
        );
        assert_eq!(canonicalize_column_name("\"Use event_day@1\""), "event_day");
        assert_eq!(canonicalize_column_name("status@2"), "status");
    }

    #[test]
    fn extracts_predicates_from_slot_qualified_use_columns() {
        let filters = vec![col("status@2").eq(lit("paid")).and(
            col("Use event_day@1")
                .in_list(vec![lit(20i64), lit(21i64), lit(22i64), lit(23i64)], false),
        )];
        let summary = extract_predicate_summary(filters.as_slice());
        let status = summary.column("status").expect("status predicate");
        assert_eq!(status.equals.len(), 1);
        let day = summary.column("event_day").expect("event_day predicate");
        assert_eq!(day.equals.len(), 4);
        assert_eq!(summary.recognized_terms, 2);
        assert_eq!(summary.residual_terms, 0);
    }
}

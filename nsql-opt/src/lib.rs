use nsql_plan::Plan;

pub fn optimize<S>(stmt: Box<Plan<S>>) -> Box<Plan<S>> {
    stmt
}

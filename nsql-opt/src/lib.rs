use nsql_plan::Plan;

pub fn optimize(stmt: Box<Plan>) -> Box<Plan> {
    stmt
}

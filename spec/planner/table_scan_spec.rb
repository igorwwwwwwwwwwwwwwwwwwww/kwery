require 'kwery'

RSpec.describe Kwery::Planner do
  it "performs a table scan by default" do
    schema = Kwery::Schema.new
    schema.create_table(:users)

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project, Kwery::Executor::TableScan]
    )
  end
end

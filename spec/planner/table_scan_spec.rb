require 'kwery'

RSpec.describe Kwery::Planner do
  it "performs a table scan by default" do
    catalog = Kwery::Catalog.new
    catalog.table :users, Kwery::Catalog::Table.new

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project, Kwery::Executor::TableScan]
    )
  end
end

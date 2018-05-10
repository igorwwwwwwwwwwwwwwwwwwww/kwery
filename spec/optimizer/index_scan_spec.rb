require 'kwery'

RSpec.describe Kwery::Optimizer do
  it "performs an index scan for order by" do
    catalog = Kwery::Catalog.new
    catalog.table :users, Kwery::Catalog::Table.new(
      columns: {
        id:     Kwery::Catalog::Column.new(:integer),
        name:   Kwery::Catalog::Column.new(:string),
        active: Kwery::Catalog::Column.new(:boolean),
      },
      indexes: [:users_idx_id],
    )
    catalog.index :users_idx_id, Kwery::Catalog::Index.new(:users, [
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      # where: [
      #   Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      # ],
      order_by: [
        Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project, Kwery::Executor::IndexScan]
    )
  end
end

require 'kwery'
require 'csv'

RSpec.describe Kwery do
  it "executes a query" do
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
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
    ])

    schema = catalog.new_schema

    importer = Kwery::Importer.new(catalog, schema)
    importer.load(:users, 'data/users.csv')

    schema.reindex(:users, :users_idx_id)

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id),
        name: Kwery::Expr::Column.new(:name),
      },
      from: :users,
      where: [
        Kwery::Expr::Gt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:active), Kwery::Expr::Literal.new(true)),
      ],
      limit: 10,
    )

    plan = query.plan(catalog)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 11, name: "Darryl"},
      {id: 12, name: "Galena"},
      {id: 19, name: "Lydia"},
      {id: 21, name: "Cara"},
      {id: 24, name: "Murphy"},
      {id: 26, name: "Ferris"},
      {id: 28, name: "Kaitlin"},
      {id: 30, name: "Russell"},
      {id: 31, name: "Heather"},
      {id: 34, name: "Lewis"},
    ])

    # TODO: missing index prefix-matching support
    # expect(context.stats).to eq({
    #   index_tuples_scanned: 10,
    #   index_comparisons: 14,
    # })
  end
end

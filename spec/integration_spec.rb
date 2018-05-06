require 'kwery'
require 'csv'

RSpec.describe Kwery do
  it "returns executes a query" do
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

    schema = {}
    catalog.tables.each do |table_name, t|
      schema[table_name] = []
    end
    catalog.indexes.each do |index_name, i|
      schema[index_name] = Kwery::Index.new
    end

    importer = Kwery::Importer.new(catalog, schema)
    importer.load(:users, 'data/users.csv')

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

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 1,  name:"Kathleen"},
      {id: 2,  name:"Xantha"},
      {id: 3,  name:"Hope"},
      {id: 4,  name:"Hedley"},
      {id: 5,  name:"Reese"},
      {id: 6,  name:"Emi"},
      {id: 7,  name:"Herrod"},
      {id: 8,  name:"Quincy"},
      {id: 9,  name:"Uta"},
      {id: 10, name:"Anastasia"}
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 10,
    })
  end
end

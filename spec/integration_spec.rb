require 'kwery'
require 'csv'

RSpec.describe Kwery do
  it "executes a query" do
    schema = Kwery::Schema.new
    schema.import_csv(:users, 'data/users.csv', { id: :integer, active: :boolean })
    schema.create_index(:users, :users_idx_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
    ])

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

    plan = query.plan(schema)

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
    #   index_tuples_fetched: 10,
    #   index_comparisons: 14,
    # })
  end
end

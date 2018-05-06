$: << 'lib'

require 'kwery'

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
  options: { notablescan: ENV['NOTABLESCAN'] == 'true' },
)

begin
  plan = query.plan(catalog)
rescue Kwery::Plan::NoTableScanError => e
  pp query
  puts
  puts "error: #{e}"
  exit 1
end

mode = ARGV.shift || 'run'
case mode
when 'query'
  pp query
when 'plan'
  pp plan
when 'run'
  context = Kwery::Plan::Context.new(schema)
  plan.call(context).each do |tup|
    puts tup
  end
  puts
  puts "stats: #{context.stats}"
else
  raise "invalid mode #{mode}"
end

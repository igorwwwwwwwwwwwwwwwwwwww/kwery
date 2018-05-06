$: << 'lib'

require 'kwery'
require 'csv'

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

context = {}
catalog.tables.each do |table_name, t|
  context[table_name] = []
end
catalog.indexes.each do |index_name, i|
  # TODO create index with custom comparator based on sort order
  context[index_name] = Kwery::Index.new
end

catalog.tables.each do |table_name, t|
  if File.exists?("#{table_name}.csv")
    csv = CSV.table("#{table_name}.csv", converters: nil)
    csv.each do |row|
      tup = t.tuple(row)

      table = context[table_name]
      table << tup
      tid = table.size - 1

      t.indexes.each do |index_name|
        index = context[index_name]

        key = catalog.indexes[index_name].indexed_exprs.map(&:expr).map { |expr| expr.call(tup) }
        index.insert(key, tid)
      end
    end
  end
end

# SELECT name
# FROM users
# WHERE active = true
# ORDER BY id DESC
# LIMIT 10

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
rescue Kwery::Query::NoTableScanError => e
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
  plan.call(context).each do |tup|
    puts tup
  end
else
  raise "invalid mode #{mode}"
end

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
])

schema = catalog.new_schema

importer = Kwery::Importer.new(catalog, schema)
importer.load(:users, 'data/users.csv')

schema.reindex(:users, :users_idx_id)

sql = ARGV.shift

unless sql
  puts 'Usage:'
  puts '  ruby engine.rb <sql>'
  exit 1
end

options = {}
options[:notablescan] = true if ENV['NOTABLESCAN'] == 'true'

parser = Kwery::Parser.new(options)
query = parser.parse(sql, ENV['DEBUG'] == 'true')

plan = query.plan(catalog)

context = Kwery::Executor::Context.new(schema)
plan.call(context).each do |tup|
  if tup[:_pretty]
    tup.delete(:_pretty)
    pp tup
  else
    puts tup
  end
end
puts
puts "stats: #{context.stats}"

$: << 'lib'

require 'kwery'
require 'csv'

catalog = Kwery::Catalog.new

catalog.table :users do |t|
  t.column :id, :integer
  t.column :name, :string
  t.column :active, :boolean
  t.index :users_idx_id, [:users, :id, :asc], [:users, :active, :asc]
end

context = {}
catalog.tables.each do |table_name, t|
  context[table_name] = []
  context = context.merge(
    # TODO create index with custom comparator based on sort order
    t.indexes.keys.map { |k| [k, Kwery::Index.new] }.to_h
  )
end

catalog.tables.each do |table_name, t|
  if File.exists?("#{table_name}.csv")
    csv = CSV.table("#{table_name}.csv", converters: nil)
    csv.each do |row|
      tup = t.tuple(row)

      table = context[table_name]
      table << tup
      tid = table.size - 1

      t.indexes.each do |index_name, i|
        index = context[index_name]

        key = i.columns.map(&:expr).map { |expr| expr.call(tup) }
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
    id: Kwery::Query::Field.new(:id),
    name: Kwery::Query::Field.new(:name),
  },
  from: :users,
  where: [
    Kwery::Query::Gt.new(Kwery::Query::Field.new(:id), Kwery::Query::Literal.new(10)),
    Kwery::Query::Eq.new(Kwery::Query::Field.new(:active), Kwery::Query::Literal.new(true)),
  ],
  limit: 10,
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

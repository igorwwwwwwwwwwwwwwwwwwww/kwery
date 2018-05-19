require 'kwery'

RSpec.describe Kwery::Planner do
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
      order_by: [
        Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id, {}]]
    )
  end

  it "performs an index scan for where" do
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
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id,
          {eq: [10]}]]
    )
  end

  it "matches an indexed expression" do
    catalog = Kwery::Catalog.new
    catalog.table :users, Kwery::Catalog::Table.new(
      columns: {
        id:     Kwery::Catalog::Column.new(:integer),
        name:   Kwery::Catalog::Column.new(:string),
        active: Kwery::Catalog::Column.new(:boolean),
      },
      indexes: [:users_idx_upper_name],
    )
    catalog.index :users_idx_upper_name, Kwery::Catalog::Index.new(:users, [
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Upper.new(Kwery::Expr::Column.new(:name)), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Upper.new(Kwery::Expr::Column.new(:name)), Kwery::Expr::Literal.new('CARA')),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_upper_name,
          {eq: ['CARA']}]]
    )
  end

  it "matches a compound index" do
    catalog = Kwery::Catalog.new
    catalog.table :users, Kwery::Catalog::Table.new(
      columns: {
        id:     Kwery::Catalog::Column.new(:integer),
        name:   Kwery::Catalog::Column.new(:string),
        active: Kwery::Catalog::Column.new(:boolean),
      },
      indexes: [:users_idx_name_active],
    )
    catalog.index :users_idx_name_active, Kwery::Catalog::Index.new(:users, [
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:active), Kwery::Expr::Literal.new(true)),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_name_active,
          {eq: ['Cara', true]}]]
    )
  end

  it "matches a compound index with reverse field order" do
    catalog = Kwery::Catalog.new
    catalog.table :users, Kwery::Catalog::Table.new(
      columns: {
        id:     Kwery::Catalog::Column.new(:integer),
        name:   Kwery::Catalog::Column.new(:string),
        active: Kwery::Catalog::Column.new(:boolean),
      },
      indexes: [:users_idx_active_name],
    )
    catalog.index :users_idx_active_name, Kwery::Catalog::Index.new(:users, [
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:active), Kwery::Expr::Literal.new(true)),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_active_name,
          {eq: [true, 'Cara']}]]
    )
  end

  xit "matches an index prefix" do
    catalog = Kwery::Catalog.new
    catalog.table :users, Kwery::Catalog::Table.new(
      columns: {
        id:     Kwery::Catalog::Column.new(:integer),
        name:   Kwery::Catalog::Column.new(:string),
        active: Kwery::Catalog::Column.new(:boolean),
      },
      indexes: [:users_idx_name_active],
    )
    catalog.index :users_idx_name_active, Kwery::Catalog::Index.new(:users, [
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_name_active,
          {eq: ['Cara']}]]
    )
  end

  it "matches a >= constraint" do
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
      where: [
        Kwery::Expr::Gt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id,
          {gt: [10]}]]
    )
  end

  xit "matches a >= constraint with an index prefix" do
    catalog = Kwery::Catalog.new
    catalog.table :users, Kwery::Catalog::Table.new(
      columns: {
        id:     Kwery::Catalog::Column.new(:integer),
        name:   Kwery::Catalog::Column.new(:string),
        active: Kwery::Catalog::Column.new(:boolean),
      },
      indexes: [:users_idx_active_id],
    )
    catalog.index :users_idx_active_id, Kwery::Catalog::Index.new(:users, [
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:active), Kwery::Expr::Literal.new(true)),
        Kwery::Expr::Gt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_active_id,
          {gt: [true, 10]}]]
    )
  end

  it "matches a < constraint" do
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
      where: [
        Kwery::Expr::Lt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id,
          {lt: [10]}]]
    )
  end

  it "matches a between constraint" do
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
      where: [
        Kwery::Expr::Gt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
        Kwery::Expr::Lt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(15)),
      ],
    )

    plan = query.plan(catalog)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id,
          {gt: [10], lt: [15]}]]
    )
  end

  it "matches order by and an eq constraint prefix" do
    catalog = Kwery::Catalog.new
    catalog.table :users, Kwery::Catalog::Table.new(
      columns: {
        id:     Kwery::Catalog::Column.new(:integer),
        name:   Kwery::Catalog::Column.new(:string),
        active: Kwery::Catalog::Column.new(:boolean),
      },
      indexes: [:users_idx_name_id],
    )
    catalog.index :users_idx_name_id, Kwery::Catalog::Index.new(:users, [
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
      ],
      order_by: [
        Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
      ],
    )

    plan = query.plan(catalog)

    # TODO: is the executor even able to run this plan?
    #       this would require prefix seeking in the binary search tree.
    #       ranges are easier because those are full gt/lt sargs.
    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_name_id,
          {eq: ['Cara']}]]
    )
  end

  it "matches eq constraint prefix with gt" do
    catalog = Kwery::Catalog.new
    catalog.table :users, Kwery::Catalog::Table.new(
      columns: {
        id:     Kwery::Catalog::Column.new(:integer),
        name:   Kwery::Catalog::Column.new(:string),
        active: Kwery::Catalog::Column.new(:boolean),
      },
      indexes: [:users_idx_name_id],
    )
    catalog.index :users_idx_name_id, Kwery::Catalog::Index.new(:users, [
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
      Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
        Kwery::Expr::Gt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(catalog)

    # TODO: what is the lt condition for the upper bound even supposed to look like?
    #         i really did not think this through...
    #       what rdbms do is only use the index to find the starting point,
    #         and then have a condition to stop.
    #       i guess we could use eq for this purpose, with some changes in the
    #         binary search tree. or we could introduce a new prefix_eq, or prefix_cond
    #         argument.
    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_name_id,
          {prefix: ['Cara'], gt: ['Cara', 10]}]]
    )
  end
end

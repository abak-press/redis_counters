# RedisCounters

[![Build Status](https://drone.railsc.ru/api/badges/abak-press/redis_counters/status.svg)](https://drone.railsc.ru/abak-press/redis_counters)
[![Code Climate](https://codeclimate.com/github/abak-press/redis_counters/badges/gpa.svg)](https://codeclimate.com/github/abak-press/redis_counters)
[![Test Coverage](https://codeclimate.com/github/abak-press/redis_counters/badges/coverage.svg)](https://codeclimate.com/github/abak-press/redis_counters/coverage)

Набор структур данных на базе Redis.

## RedisCounters::HashCounter

Счетчик на основе Hash, с ~~преферансом и тайками-близняшками~~ партиционированием и кластеризацией значений.

Обязательные параметры: counter_name, field_name или group_keys.

### Сложность
  + инкремент - O(1).

### Примеры использования

Простой счетчик значений.
```ruby
counter = RedisCounters::HashCounter.new(redis, {
  :counter_name => :simple_counter,
  :field_name   => :pages
})

5.times { counter.increment }

redis:
  simple_counter = {
    pages => 5
  }

> counter.partitions
=> [{}]

> counter.data
=> [{:value=>5}]
```

Счетчик посещенных страниц компании с партиционированием по дате.
```ruby
counter = RedisCounters::HashCounter.new(redis, {
  :counter_name   => :pages_by_day,
  :group_keys     => [:company_id],
  :partition_keys => [:date]
})

2.times { counter.increment(:company_id => 1, :date => '2013-08-01') }
3.times { counter.increment(:company_id => 2, :date => '2013-08-01') }
1.times { counter.increment(:company_id => 3, :date => '2013-08-02') }

redis:
  pages_by_day:2013-08-01 = {
    1 => 2
    2 => 3
  }
  pages_by_day:2013-08-02 = {
    3 => 1
  }

> counter.partitions
=> [{:date=>"2013-08-01"}, {:date=>"2013-08-02"}]

> counter.data
=> [{:company_id=>"1", :value=>2},
 {:company_id=>"2", :value=>3},
 {:company_id=>"3", :value=>1}]

> counter.delete_partitions!(:date => '2013-08-01')
=> [1]

> counter.partitions
=> [{:date=>"2013-08-02"}]

> counter.data
=> [{:company_id=>"3", :value=>1}]

> counter.delete_all!
=> [1]

> counter.data
=> []
```

Тоже самое, но партиция задается с помощью proc.
```ruby
counter = RedisCounters::HashCounter.new(redis, {
  :counter_name   => :pages_by_day,
  :group_keys     => [:company_id],
  :partition_keys => proc { |params| params.fetch(:date) }
})
```

Счетчик посещенных страниц с группировкой по городу посетителя и партиционированием по дате и компании.
```ruby
counter = RedisCounters::HashCounter.new(redis, {
  :counter_name   => :pages_by_day_city,
  :group_keys     => [:company_id, :city_id],
  :partition_keys => [:date, :company_id]
})

2.times { counter.increment(:date => '2013-08-01', :company_id => 1, :city_id => 11) }
1.times { counter.increment(:date => '2013-08-01', :company_id => 1, :city_id => 12) }
4.times { counter.increment(:date => '2013-08-01', :company_id => 2, :city_id => 10) }
3.times { counter.increment(:date => '2013-08-02', :company_id => 1, :city_id => 15) }

redis:
  pages_by_day_city:2013-08-01:1 = {
    1:11 => 2,
    1:12 => 1
  }

  pages_by_day_city:2013-08-01:2 = {
    2:10 => 4
  }

  pages_by_day_city:2013-08-02:1 = {
    1:15 => 3
  }

> counter.partitions
=> [{:date=>"2013-08-02", :company_id=>"1"},
 {:date=>"2013-08-01", :company_id=>"1"},
 {:date=>"2013-08-01", :company_id=>"2"}]

> counter.partitions(:date => '2013-08-01')
=> [{:date=>"2013-08-01", :company_id=>"1"},
 {:date=>"2013-08-01", :company_id=>"2"}]

> counter.data
=> [{:company_id => 1, :city_id=>"15", :value=>3},
 {:company_id => 1, :city_id=>"11", :value=>2},
 {:company_id => 1, :city_id=>"12", :value=>1},
 {:company_id => 2, :city_id=>"10", :value=>4}]

> counter.data(:date => '2013-08-01')
=> [{:company_id => 1, :city_id=>"11", :value=>2},
 {:company_id => 1, :city_id=>"12", :value=>1},
 {:company_id => 2, :city_id=>"10", :value=>4}]

> counter.data(:date => '2013-08-01') { |batch| puts batch }
{:company_id => 1, :city_id=>"11", :value=>2}
{:company_id => 1, :city_id=>"12", :value=>1}
{:company_id => 2, :city_id=>"10", :value=>4}
```

## RedisCounters::UniqueValuesLists::Blocking

Список уникальных значений, с возможностью кластеризации и партиционирования значений.

Особенности:
- Использует механизм оптимистичных блокировок.
- Помимо списка значений, ведет так же, список партиций, для каждого кластера.
- Полностью транзакционен - сторонний блок, выполняемый после добавления уникального элемента,
  выполняется в той же транзакции, в которой добавляется уникальный элемент.

Вероятно, в условиях большой конкурентности, обладает не лучшей производительносью из-за частых блокировок.

Обязательные параметры: counter_name и value_keys.

### Сложность
  + добавление элемента - от O(1), при отсутствии партиционирования, до O(N), где N - кол-во партиций.

### Примеры использования

Простой список уникальных пользователей.
```ruby
counter = RedisCounters::UniqueValuesLists::Blocking.new(redis, {
  :counter_name => :users,
  :value_keys   => [:user_id]
})

counter.increment(:user_id => 1)
counter.increment(:user_id => 2)
counter.increment(:user_id => 1)

redis:
  users = ['1', '2']
```

Список уникальных пользователей, посетивших компаниию, за месяц, партиционированный по суткам.
```ruby
counter = RedisCounters::UniqueValuesLists::Blocking.new(redis, {
  :counter_name   => :company_users_by_month,
  :value_keys     => [:company_id, :user_id],
  :cluster_keys   => [:start_month_date],
  :partition_keys => [:date]
})

2.times { counter.add(:company_id => 1, :user_id => 11, :date => '2013-08-10', :start_month_date => '2013-08-01') }
3.times { counter.add(:company_id => 1, :user_id => 22, :date => '2013-08-10', :start_month_date => '2013-08-01') }
3.times { counter.add(:company_id => 1, :user_id => 22, :date => '2013-09-05', :start_month_date => '2013-09-01') }
3.times { counter.add(:company_id => 2, :user_id => 11, :date => '2013-08-10', :start_month_date => '2013-08-01') }
1.times { counter.add(:company_id => 2, :user_id => 22, :date => '2013-08-11', :start_month_date => '2013-08-01') }

redis:
  company_users_by_month:2013-08-01:partitions = ['2013-08-10', '2013-08-11']
  company_users_by_month:2013-08-01:2013-08-10 = ['1:11', '1:22', '2:11']
  company_users_by_month:2013-08-01:2013-08-11 = ['2:22']

  company_users_by_month:2013-09-01:partitions = ['2013-09-05']
  company_users_by_month:2013-09-01:2013-09-05 = ['1:22']
```

## RedisCounters::UniqueValuesLists::NonBlocking

Быстрый список уникальных значений, с возможностью кластеризации и партиционирования значений.

Скорость работы достигается за счет следующих особенностей:
- Использует 2х объема памяти для хранения элементов,
при использовании партиционирования.
Eсли партиционирование не используется, то расход памяти такой-же как у UniqueValuesLists::Blocking.
- Не транзакционен - сторонний блок, выполняемый после добавления уникального элемента,
выполняется за пределами транзакции, в которой добавляется уникальный элемент.
- Не ведется список партиций.

Обязательные параметры: counter_name и value_keys.

### Сложность
  + добавление элемента - O(1)


## RedisCounters::UniqueValuesLists::Expirable

Список уникальных значений, с возможностью expire отдельных элементов.

На основе сортированного множества.
http://redis4you.com/code.php?id=010

На основе механизма оптимистических блокировок.
смотри Optimistic locking using check-and-set:
http://redis.io/topics/transactions

Особенности:
- Expire - таймаут, можно установить как на уровне счетчика,
  так и на уровне отдельного занчения;
- Очистка возможна как в автоматическогом режиме так в и ручном;
- Значения сохраняет в партициях;
- Ведет список партиций;
- Полностью транзакционен.

Обязательные параметры: counter_name и value_keys.
Таймаут задается параметром :expire. По умолчанию :never.
:clean_expired - режим автоочистки. По умолчанию true.

### Примеры использования

```ruby
counter = RedisCounters::UniqueValuesLists::Expirable.new(redis,
  :counter_name => :sessions,
  :value_keys   => [:session_id],
  :expire       => 10.minutes
)

counter << session_id: 1
counter << session_id: 2
counter << session_id: 3, expire: :never

counter.data
> [{session_id: 1}, {session_id: 2}, {session_id: 3}]

# after 10 minutes

counter.data
> [{session_id: 3}]

counter.has_value?(session_id: 1)
false
```

## RedisCounters::UniqueHashCounter

Сборная конструкция на основе предыдущих.
HashCounter, с возможностью подсчета только у уникальных событий.

### Сложность
  аналогично сложности, используемого уникального списка.

### Примеры использования

Счетчик уникальных пользователей, посетивших компаниию, за месяц, кластеризованный по суткам.
```ruby
counter = RedisCounters::UniqueHashCounter.new(redis, {
  :counter_name   => :company_users_by_month,
  :group_keys     => [:company_id],
  :partition_keys => [:date],
  :unique_list => {
    :list_class     => RedisCounters::UniqueValuesLists::Blocking
    :value_keys     => [:company_id, :user_id],
    :cluster_keys   => [:start_month_date],
    :partition_keys => [:date]
  }
})

2.times { counter.increment(:company_id => 1, :user_id => 11, :date => '2013-08-10', :start_month_date => '2013-08-01') }
3.times { counter.increment(:company_id => 1, :user_id => 22, :date => '2013-08-10', :start_month_date => '2013-08-01') }
3.times { counter.increment(:company_id => 1, :user_id => 22, :date => '2013-09-05', :start_month_date => '2013-09-01') }
3.times { counter.increment(:company_id => 2, :user_id => 11, :date => '2013-08-10', :start_month_date => '2013-08-01') }
1.times { counter.increment(:company_id => 2, :user_id => 22, :date => '2013-08-11', :start_month_date => '2013-08-01') }

redis:
  company_users_by_month:2013-08-10 = {
    1 = 2,
    2 = 1
  }
  company_users_by_month:2013-08-11 = {
    2 = 1
  }
  company_users_by_month:2013-09-05 = {
    1 = 1
  }

  company_users_by_month_uq:2013-08-01:partitions = ['2013-08-10', '2013-08-11']
  company_users_by_month_uq:2013-08-01:2013-08-10 = ['1:11', '1:22', '2:11']
  company_users_by_month_uq:2013-08-01:2013-08-11 = ['2:22']

  company_users_by_month_uq:2013-09-01:partitions = ['2013-09-05']
  company_users_by_month_uq:2013-09-01:2013-09-05 = ['1:22']
```

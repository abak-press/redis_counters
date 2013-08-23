# RedisCounters [![Code Climate](https://codeclimate.com/repos/522e9b497e00a46a0d01227c/badges/ae868ca76e52852ebc5a/gpa.png)](https://codeclimate.com/repos/522e9b497e00a46a0d01227c/feed) [![CircleCI](https://circleci.com/gh/abak-press/class_logger.png?circle-token=e4d0ed5c60a5ff795bf971229addb871552c2750)](https://circleci.com/gh/abak-press/redis_counters)

Набор структур данных на базе Redis.

## RedisCounters::HashCounter

Счетчик на основе Hash, с ~~преферансом и тайками-близняшками~~ партиционированием и группировкой значений.

Обязательные параметры: counter_name, field_name или group_keys.

### Сложность
  + инкремент - O(1).

### Примеры использования

Простой счетчик значений.
```ruby
counter = RedisCounters::HashCounter.new(redis, {
  :counter_name => :pages_by_day,
  :field_name   => :pages
})

5.times { counter.increment }

redis:
  pages_by_day = {
    pages => 5
  }
```

Счетчик посещенных страниц компании с партиционированием по дате.
```ruby
counter = RedisCounters::HashCounter.new(redis, {
  :counter_name   => :pages_by_day,
  :group_keys     => [:company_id],
  :partition_keys => [:date]
})

2.times { counter.increment(:company_id = 1, :date => '2013-08-01') }
3.times { counter.increment(:company_id = 2, :date => '2013-08-01') }
1.times { counter.increment(:company_id = 3, :date => '2013-08-02') }

redis:
  pages_by_day:2013-08-01 = {
    1 => 2
    2 => 3
  }
  pages_by_day:2013-08-02 = {
    3 => 1
  }
```

Тоже самое, но партиция задается с помощью proc.
```ruby
counter = RedisCounters::HashCounter.new(redis, {
  :counter_name   => :pages_by_day,
  :group_keys     => [:company_id],
  :partition_keys => proc { |params| params.fetch(:date) }
})
```

Счетчик посещенных страниц компании с группировкой по городу посетителя и партиционированием по дате.
```ruby
counter = RedisCounters::HashCounter.new(redis, {
  :counter_name   => :pages_by_day,
  :group_keys     => [:company_id, city_id],
  :partition_keys => [:date]
})

2.times { counter.increment(:company_id = 1, :city_id => 11, :date => '2013-08-01') }
1.times { counter.increment(:company_id = 1, :city_id => 12, :date => '2013-08-01') }
3.times { counter.increment(:company_id = 2, :city_id => 11, :date => '2013-08-01') }

redis:
  pages_by_day:2013-08-01 = {
    1:11 => 2,
    1:12 => 1,
    2_11 => 3
  }
```

## RedisCounters::UniqueValuesList

Список уникальных значений, с возможностью группировки и партиционирования значений.
Помимо списка значений, ведет так же, список партиций, для каждой группы.

Обязательные параметры: counter_name и value_keys.

### Сложность
  + добавление элемента - от O(1), при отсутствии партиционирования, до O(N), где N - кол-во партиций.

### Примеры использования

Простой список уникальных пользователей.
```ruby
counter = RedisCounters::UniqueValuesList.new(redis, {
  :counter_name => :users,
  :value_keys   => [:user_id]
})

counter.increment(:user_id => 1)
counter.increment(:user_id => 2)
counter.increment(:user_id => 1)

redis:
  users = ['1', '2']
```

Список уникальных пользователей, посетивших компаниию, за месяц, сгруппированный по суткам.
```ruby
counter = RedisCounters::UniqueValuesList.new(redis, {
  :counter_name   => :company_users_by_month,
  :value_keys     => [:company_id, :user_id],
  :group_keys     => [:start_month_date],
  :partition_keys => [:date]
})

2.times { counter.add(:company_id = 1, :user_id => 11, :date => '2013-08-10', :start_month_date => '2013-08-01') }
3.times { counter.add(:company_id = 1, :user_id => 22, :date => '2013-08-10', :start_month_date => '2013-08-01') }
3.times { counter.add(:company_id = 1, :user_id => 22, :date => '2013-09-05', :start_month_date => '2013-09-01') }
3.times { counter.add(:company_id = 2, :user_id => 11, :date => '2013-08-10', :start_month_date => '2013-08-01') }
1.times { counter.add(:company_id = 2, :user_id => 22, :date => '2013-08-11', :start_month_date => '2013-08-01') }

redis:
  company_users_by_month:2013-08-01:partitions = ['2013-08-10', '2013-08-11']
  company_users_by_month:2013-08-01:2013-08-10 = ['1:11', '1:22', '2:11']
  company_users_by_month:2013-08-01:2013-08-11 = ['2:22']

  company_users_by_month:2013-09-01:partitions = ['2013-09-05']
  company_users_by_month:2013-09-01:2013-09-05 = ['1:22']
```

## RedisCounters::UniqueHashCounter

Структура на основе двух предыдущих.
HashCounter, с возможностью подсчета только у уникальных событий.

### Сложность
  аналогично UniqueValuesList.

### Примеры использования

Счетчик уникальных пользователей, посетивших компаниию, за месяц, сгруппированный по суткам.
```ruby
counter = RedisCounters::UniqueHashCounter.new(redis, {
  :counter_name   => :company_users_by_month,
  :group_keys     => [:company_id],
  :partition_keys => [:date],
  :unique_list => {
    :value_keys     => [:company_id, :user_id],
    :group_keys     => [:start_month_date],
    :partition_keys => [:date]
  }
})

2.times { counter.increment(:company_id = 1, :user_id => 11, :date => '2013-08-10', :start_month_date => '2013-08-01') }
3.times { counter.increment(:company_id = 1, :user_id => 22, :date => '2013-08-10', :start_month_date => '2013-08-01') }
3.times { counter.increment(:company_id = 1, :user_id => 22, :date => '2013-09-05', :start_month_date => '2013-09-01') }
3.times { counter.increment(:company_id = 2, :user_id => 11, :date => '2013-08-10', :start_month_date => '2013-08-01') }
1.times { counter.increment(:company_id = 2, :user_id => 22, :date => '2013-08-11', :start_month_date => '2013-08-01') }

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
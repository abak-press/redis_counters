# v1.5.2

* 2022-05-05 [9c3a157](../../commit/9c3a157) - __(Igor Prudnikov)__ Release 1.5.2 
* 2022-05-05 [fb05645](../../commit/fb05645) - __(Igor Prudnikov)__ fix: fix hash counter with delimeters containing non-utf8 data 
https://jira.railsc.ru/browse/BPC-20533

* 2022-05-05 [e10e51d](../../commit/e10e51d) - __(Igor Prudnikov)__ chore: remove older ruby versions support 

# v1.5.1

* 2021-08-06 [09ed078](../../commit/09ed078) - __(TamarinEA)__ chore: support ruby 2.4 and use redis in test 

# v1.5.0

* 2017-04-21 [d64da5e](../../commit/d64da5e) - __(Pavel Galkin)__ feat: support two value delimiters 
https://jira.railsc.ru/browse/CK-862

* 2017-04-21 [a4f458c](../../commit/a4f458c) - __(Pavel Galkin)__ chore: update infrastructure 

# v1.4.0

* 2016-07-20 [e6be297](../../commit/e6be297) - __(Stanislav Gordanov)__ feat: переместит значение инкремента в начало массива данных 
* 2015-07-26 [e4771c5](../../commit/e4771c5) - __(Artem Napolskih)__ Update README.md 
* 2015-02-19 [c86a1aa](../../commit/c86a1aa) - __(bibendi)__ commit changelog after generate it 
* 2015-02-19 [b5ec9f8](../../commit/b5ec9f8) - __(bibendi)__ change releasing tasks to upload on rubygems 

# v1.2.0

* 2014-11-10 [6257dc2](../../commit/6257dc2) - __(Artem Napolskih)__ feature(hash_counter): added float mode 
* 2014-10-17 [48deb40](../../commit/48deb40) - __(Artem Napolskih)__ feature(unique_values_list): introduced a list of unique values to expiry of the members 
* 2014-10-17 [e1206b7](../../commit/e1206b7) - __(Artem Napolskih)__ feature(unique_values_list): introduce has_value? method 
* 2014-10-17 [cccc2be](../../commit/cccc2be) - __(Artem Napolskih)__ feature(unique_values_list): rename Standard and Fast unique values list to Blocking and NonBlocking respectively 
* 2014-10-17 [7521fd8](../../commit/7521fd8) - __(Artem Napolskih)__ chore(specs): reorganize unique lists specs 
* 2014-04-23 [94fada3](../../commit/94fada3) - __(Artem Napolskih)__ chore(all): fix circleci badge 
* 2013-11-20 [0ae5531](../../commit/0ae5531) - __(Artem Napolskih)__ Release 1.1.0 

# v1.1.0

* 2013-11-20 [3897988](../../commit/3897988) - __(Artem Napolskih)__ refactor(all): back delete_all! methods 
* 2013-11-15 [2d1c733](../../commit/2d1c733) - __(Artem Napolskih)__ refactor(all): extract Cluster and Partition classes 
* 2013-10-22 [020cc8e](../../commit/020cc8e) - __(Napolskih)__ refactor(all): shared code of clustering and partitioning extracted into module - group_keys -> cluster_keys - shared code of clustering and partitioning extracted into module 

# v1.0.1

* 2013-10-21 [bfa328a](../../commit/bfa328a) - __(Artem Napolskih)__ feature(hash_counter): method date when called with a block, now returns the total number of lines 
* 2013-10-21 [6d04c95](../../commit/6d04c95) - __(Artem Napolskih)__ Release 1.0.0 
* 2013-10-10 [b29eab7](../../commit/b29eab7) - __(Artem Napolskih)__ feature(unique_values_lists): added methods to read and delete data 
* 2013-10-10 [67c5200](../../commit/67c5200) - __(Artem Napolskih)__ feature(unique_hash_counter): introduce unique list postfix delimiter 
* 2013-10-08 [8a9a2a9](../../commit/8a9a2a9) - __(Artem Napolskih)__ feature(hash_counter): added methods to read and delete data 
* 2013-09-13 [f8d037c](../../commit/f8d037c) - __(Artem Napolskih)__ docs(all): documenting 

# v1.0.0beta1

# v1.0.0

* 2013-10-10 [b29eab7](../../commit/b29eab7) - __(Artem Napolskih)__ feature(unique_values_lists): added methods to read and delete data 
* 2013-10-10 [67c5200](../../commit/67c5200) - __(Artem Napolskih)__ feature(unique_hash_counter): introduce unique list postfix delimiter 
* 2013-10-08 [8a9a2a9](../../commit/8a9a2a9) - __(Artem Napolskih)__ feature(hash_counter): added methods to read and delete data 
* 2013-09-13 [f8d037c](../../commit/f8d037c) - __(Artem Napolskih)__ docs(all): documenting 
* 2013-09-12 [0a176fa](../../commit/0a176fa) - __(Artem Napolskih)__ feature(all) introduce key and value delimiters 
* 2013-09-12 [addbb40](../../commit/addbb40) - __(Artem Napolskih)__ feature(unique_values_list): fast (non blocking) unique values list added 
- refactoring unique_values_list;
- fast (non blocking) unique values list added;
- unique_list[:list_class] property introduced on UniqueHashCounter.

* 2013-08-23 [897418d](../../commit/897418d) - __(Artem Napolskih)__ Initial commit 
* 2013-08-21 [21ed53b](../../commit/21ed53b) - __(napolskih)__ dummy commit 

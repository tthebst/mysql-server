/* Copyright (c) 2004, 2021, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_kvpmem.cc

  @brief
  The ha_kvpmem engine is a stubbed storage engine for kvpmem purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/kvpmem/ha_kvpmem.h.

  @details
  ha_kvpmem will let you create/open/delete tables, but
  nothing further (for kvpmem, indexes are not supported nor can data
  be stored in the table). Use this kvpmem as a template for
  implementing the same functionality in your own storage engine. You
  can enable the kvpmem storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-kvpmem-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE \<table name\> (...) ENGINE=KVPMEM;

  The kvpmem storage engine is set up to use table locks. It
  implements an kvpmem "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  kvpmem handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_kvpmem.h before reading the rest
  of this file.

  @note
  When you create an KVPMEM table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an kvpmem select that would do a scan of an entire
  table:

  @code
  ha_kvpmem::store_lock
  ha_kvpmem::external_lock
  ha_kvpmem::info
  ha_kvpmem::rnd_init
  ha_kvpmem::extra
  ha_kvpmem::rnd_next
  ha_kvpmem::rnd_next
  ha_kvpmem::rnd_next
  ha_kvpmem::rnd_next
  ha_kvpmem::rnd_next
  ha_kvpmem::rnd_next
  ha_kvpmem::rnd_next
  ha_kvpmem::rnd_next
  ha_kvpmem::rnd_next
  ha_kvpmem::extra
  ha_kvpmem::external_lock
  ha_kvpmem::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the kvpmem storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_kvpmem::open() would also have been necessary. Calls to
  ha_kvpmem::extra() are hints as to what will be occurring to the request.

  A Longer KVpmem can be found called the "Skeleton Engine" which can be
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#include "storage/kvpmem/ha_kvpmem.h"

#include <atomic>
#include <libpmemkv.hpp>
#include <libpmemobj++/string_view.hpp>
#include <memory>
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "typelib.h"

static handler *kvpmem_create_handler(handlerton *hton, TABLE_SHARE *table,
                                       bool partitioned, MEM_ROOT *mem_root);

handlerton *kvpmem_hton;

// ====== GLOBAL VARS =======
pmem::kv::db *kv = nullptr;
std::mutex table_create_mutex;
std::atomic<long> index_count(1);
// ====== GLOBAL VARS =======

void print_row(char *row, size_t row_size) {
  for (size_t i = 0; i < row_size; i++) {
    DBUG_PRINT("KVDK", ("WRITE ROW char %lud: %d", i, *(row + i)));
  }
}

void print_row(uchar *row, size_t row_size) {
  for (size_t i = 0; i < row_size; i++) {
    DBUG_PRINT("KVDK", ("WRITE ROW char %lud: %d", i, *(row + i)));
  }
}

std::string toBinary(int n) {
  std::string r;
  while (n != 0) {
    r += (n % 2 == 0 ? "0" : "1");
    n /= 2;
  }
  return r;
}

void print_key(std::string sv) {
  for (int i = 0; i < sv.length(); ++i) {
    std::cout << sv[i] << ":" << toBinary(sv[i]) << " ";
  }
  std::cout << std::endl;
}

static int kvpmem_commit(handlerton *hton, /*!< in: InnoDB handlerton */
                         THD *,            /*!< in: MySQL thread handle of the
                                              user for whom the transaction should
                                              be committed */
                         bool)             /*!< in: true - commit transaction
                                                      false - the current SQL statement
                                                      ended */
{
  DBUG_TRACE;
  assert(hton == nullptr);
  DBUG_PRINT("KVDK", ("commit write batch"));

  return 0;
}

static std::string read_key(pmem::kv::db::read_iterator &it) {
  /* key_result's type is a pmem::kv::result<string_view>, for more information
   * check pmem::kv::result documentation */
  pmem::kv::result<pmem::kv::string_view> key_result = it.key();
  /* check if the result is ok, you can also do:
   * key_result == pmem::kv::status::OK
   * or
   * key_result.get_status() == pmem::kv::status::OK */
  assert(key_result.is_ok());

  // TODO improve conversion-> this causes a copy
  std::string result(key_result.get_value().data(),
                     key_result.get_value().size());

  return result;
}

static std::string read_value(pmem::kv::db::read_iterator &it) {
  /* val_result's type is a pmem::kv::result<string_view>, for more information
   * check pmem::kv::result documentation */
  pmem::kv::result<pmem::kv::string_view> val_result = it.read_range();
  /* check if the result is ok, you can also do:
   * val_result == pmem::kv::status::OK
   * or
   * val_result.get_status() == pmem::kv::status::OK */
  assert(val_result.is_ok());

  // create copy of result. val_result is not null terminated and calling data()
  // causes undefined behavior
  std::string result(val_result.get_value().data(),
                     val_result.get_value().size());

  return result;
}

static std::string create_key(const std::string table_name,
                              const std::string key) {
  std::string tot_key = table_name + "_" + key;
  return tot_key;
}

// insert element into table that marks first and last element
static void insert_table_markers(std::string *table_name) {
  pmem::kv::status s = kv->put(create_key(*table_name, ""), std::to_string(1));
  assert(s == pmem::kv::status::OK);

  s = kv->put(create_key(*table_name, "zzz"), std::to_string(1));
  assert(s == pmem::kv::status::OK);

  return;
}

void print_db() {
  if (kv == nullptr) {
    DBUG_PRINT("KVDK", ("PRINT DB: KV==nullptr"));
    return;
  }

  // std::vector<std::thread> threads;

  // // create new iterator in thread because having mutliple read iterators in
  // one thread is undefined behaviour threads.emplace_back([&]() {
  //   auto res_it = kv->new_read_iterator();
  //   assert(res_it.is_ok());
  //   auto &it = res_it.get_value();
  //   it.seek_to_first();
  //   do {
  //     auto key = read_key(it);
  //     auto value = read_value(it);
  //     DBUG_PRINT("KVDK", ("READ DB: %s:%s", key.c_str(), value.c_str()));
  //   } while (it.next() == pmem::kv::status::OK);
  // });

  // for (auto &th : threads) th.join();

  // DBUG_PRINT("KVDK", ("========== PRINT DB END ============"));
}
/* Interface to mysqld, to check system tables supported by SE */
static bool kvpmem_is_supported_system_table(const char *db,
                                             const char *table_name,
                                             bool is_sql_layer_system_table);

KVpmem_share::KVpmem_share() { thr_lock_init(&lock); }

static int kvpmem_init_func(void *p) {
  DBUG_TRACE;

  kvpmem_hton = (handlerton *)p;
  kvpmem_hton->state = SHOW_OPTION_YES;
  kvpmem_hton->create = kvpmem_create_handler;
  kvpmem_hton->flags = HTON_CAN_RECREATE;
  kvpmem_hton->commit = kvpmem_commit;
  kvpmem_hton->is_supported_system_table = kvpmem_is_supported_system_table;

  return 0;
}

/**
  @brief
  KVpmem of simple lock controls. The "share" it creates is a
  structure we will pass to each kvpmem handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

KVpmem_share *ha_kvpmem::get_share() {
  KVpmem_share *tmp_share;

  DBUG_TRACE;

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<KVpmem_share *>(get_ha_share_ptr()))) {
    tmp_share = new KVpmem_share;
    if (!tmp_share) goto err;

    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  return tmp_share;
}

//! [custom-comparator]
class lexicographical_comparator {
 public:
  int compare(pmem::kv::string_view k1, pmem::kv::string_view k2) {
    if (k1.compare(k2) == 0)
      return 0;
    else if (std::lexicographical_compare(k1.data(), k1.data() + k1.size(),
                                          k2.data(), k2.data() + k2.size()))
      return -1;
    else
      return 1;
  }

  std::string name() { return "lexicographical_comparator"; }
};

static handler *kvpmem_create_handler(handlerton *hton, TABLE_SHARE *table,
                                      bool, MEM_ROOT *mem_root) {
  DBUG_PRINT("KVDK", ("Instantiate Handler"));
  // first time using the engine. Create a handler
  if (kv == nullptr) {
    pmem::kv::config cfg;

    /* Instead of expecting already created database pool, we could simply
     * set 'create_if_missing' flag in the config, to provide a pool if needed.
     */
    pmem::kv::status s = cfg.put_path("/pmem/csmap");
    assert(s == pmem::kv::status::OK);

    s = cfg.put_size(1024UL * 1024UL * 1024UL);
    assert(s == pmem::kv::status::OK);
    s = cfg.put_create_if_missing(true);
    assert(s == pmem::kv::status::OK);
    s = cfg.put_comparator(lexicographical_comparator{});
    assert(s == pmem::kv::status::OK);

    kv = new pmem::kv::db();
    assert(kv != nullptr);
    s = kv->open("csmap", std::move(cfg));
    DBUG_PRINT("KVDK", ("Successfully created pmemkv engine %d", (int)s));
    assert(s == pmem::kv::status::OK);

    // init global variables stored in table
    // curr_table
    table_create_mutex.lock();
    std::string v;
    s = kv->get("__system___aaa", &v);
    if (s == pmem::kv::status::NOT_FOUND) {
      std::string system = "__system__";
      insert_table_markers(&system);
    }
    table_create_mutex.unlock();

    DBUG_PRINT("KVDK", ("CURR TABLE NUM AT INIT"));

    print_db();
  }
  return new (mem_root) ha_kvpmem(hton, table);
}

ha_kvpmem::ha_kvpmem(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg),
      read_it{std::make_unique<pmem::kv::db::read_iterator>(
          kv->new_read_iterator().get_value())},
      m_int_table_flags(HA_REQUIRE_PRIMARY_KEY) {}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_handler_tablename ha_kvpmem_system_tables[] = {
    {(const char *)nullptr, (const char *)nullptr}};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @retval true   Given db.table_name is supported system table.
  @retval false  Given db.table_name is not a supported system table.
*/
static bool kvpmem_is_supported_system_table(const char *db,
                                             const char *table_name,
                                             bool is_sql_layer_system_table) {
  st_handler_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table) return false;

  // Check if this is SE layer system tables
  systab = ha_kvpmem_system_tables;
  while (systab && systab->db) {
    if (systab->db == db && strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}

/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_kvpmem::open(const char *table_name, int, uint, const dd::Table *) {
  DBUG_TRACE;

  DBUG_PRINT("KVDK", ("OPEN table %s", table_name));

  // set new active table
  active_table = table_name;
  // find active idx
  DBUG_PRINT("KVDK", ("OPEN table %s", active_table.c_str()));

  assert(kv != nullptr);
  if (!(share = get_share())) return 1;
  thr_lock_data_init(&share->lock, &lock, nullptr);

  return 0;
}

/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_kvpmem::close(void) {
  DBUG_TRACE;
  DBUG_PRINT("KVDK", ("CLOSE table?"));
  return 0;
}

/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  KVpmem of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an kvpmem of extracting all of the data as strings.
  ha_berekly.cc has an kvpmem of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments. This case also applies to
  write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_kvpmem::write_row(uchar *row) {
  DBUG_TRACE;
  /*
    KVpmem of a successful write_row. We don't store the data
    anywhere; they are thrown away. A real implementation will
    probably need to do something with 'buf'. We report a success
    here, to pretend that the insert was successful.
  */
  DBUG_PRINT("KVDK", ("WRITE ROW %s", active_table.c_str()));

  // need to create stringview with correct size becuase row is not null
  // terminated
  pmem::kv::string_view sv(reinterpret_cast<char *>(row), table->s->reclength);

  char key[1024];

  size_t offset = 0;
  for (size_t i = 0;
       i < table->key_info[table->s->primary_key].user_defined_key_parts; i++) {
    strncpy(key + offset,
            reinterpret_cast<char *>(row) +
                table->key_info[table->s->primary_key].key_part[i].offset,
            table->key_info[table->s->primary_key].key_part[i].length);
    offset += table->key_info[table->s->primary_key].key_part[i].length;
  }

  std::string key_sv(key, offset);
  pmem::kv::status s = kv->put(create_key(active_table, key_sv), sv);
  assert(s == pmem::kv::status::OK);

  print_db();
  DBUG_PRINT("KVDK", ("WRITE ROW END"));
  return 0;
}

/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record. You can
  do this for kvpmem by doing:

  @code

  if (table->next_number_field && record == table->record[0])
    update_auto_increment();

  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_kvpmem::update_row(const uchar *old_row, uchar *new_row) {
  DBUG_TRACE;
  /*
    KVpmem of a successful write_row. We don't store the data
    anywhere; they are thrown away. A real implementation will
    probably need to do something with 'buf'. We report a success
    here, to pretend that the insert was successful.
  */
  DBUG_PRINT("KVDK", ("UPDATE ROW %s", active_table.c_str()));

  // remove old row
  char old_key[1024];

  // get old key from mysql row data
  size_t offset = 0;
  for (size_t i = 0;
       i < table->key_info[table->s->primary_key].user_defined_key_parts; i++) {
    strncpy(old_key + offset,
            reinterpret_cast<char *>(const_cast<uchar *>(old_row)) +
                table->key_info[table->s->primary_key].key_part[i].offset,
            table->key_info[table->s->primary_key].key_part[i].length);
    offset += table->key_info[table->s->primary_key].key_part[i].length;
  }
  std::string old_key_sv(old_key, offset);

  // prepare new row to be inserted and insert
  char new_key[1024];
  pmem::kv::string_view sv(reinterpret_cast<char *>(new_row),
                           table->s->reclength);
  // get key from mysql row data

  offset = 0;
  for (size_t i = 0;
       i < table->key_info[table->s->primary_key].user_defined_key_parts; i++) {
    strncpy(new_key + offset,
            reinterpret_cast<char *>(new_row) +
                table->key_info[table->s->primary_key].key_part[i].offset,
            table->key_info[table->s->primary_key].key_part[i].length);
    offset += table->key_info[table->s->primary_key].key_part[i].length;
  }

  std::string new_key_sv(new_key, offset);
  pmem::kv::status s;
  if (new_key_sv == old_key_sv) {
    // set read_it to null to delete read_iterator and lock on table from this
    // thread
    read_it.reset(nullptr);
    s = kv->remove(create_key(active_table, old_key_sv));
    assert(s == pmem::kv::status::OK);
    read_it = std::make_unique<pmem::kv::db::read_iterator>(
        kv->new_read_iterator().get_value());

    s = kv->put(create_key(active_table, new_key_sv), sv);
    assert(s == pmem::kv::status::OK);

    s = read_it->seek(create_key(active_table, new_key_sv));
    assert(s == pmem::kv::status::OK);
  } else {
    // move read iterator to element before this element and store key
    s = read_it->seek_lower(create_key(active_table, old_key_sv));
    auto prev_key = read_key(*read_it);
    assert(s == pmem::kv::status::OK);
    // delete read_iterator ptr. read_iterator gets destroyed and current row
    // gets unlocked sucht that it can be deleted
    read_it.reset(nullptr);
    s = kv->remove(create_key(active_table, old_key_sv));
    assert(s == pmem::kv::status::OK);

    // insert new element
    s = kv->put(create_key(active_table, new_key_sv), sv);
    assert(s == pmem::kv::status::OK);

    // create new read iterator and seek to prev key stored.
    read_it = std::make_unique<pmem::kv::db::read_iterator>(
        kv->new_read_iterator().get_value());

    s = read_it->seek(prev_key);
    assert(s == pmem::kv::status::OK);
  }

  DBUG_PRINT("KVDK", ("UPDATE ROW PUT NEW KEY %s", new_key_sv));

#ifdef DEBUG
  print_db();
#endif

  DBUG_PRINT("KVDK", ("WRITE ROW END"));
  return 0;
}

/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_kvpmem::delete_row(const uchar *) {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

int ha_kvpmem::index_read_map(uchar *, const uchar *, key_part_map,
                              enum ha_rkey_function) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  Used to read forward through the index.
*/

int ha_kvpmem::index_next(uchar *) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_kvpmem::index_prev(uchar *) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_kvpmem::index_first(uchar *) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_kvpmem::index_last(uchar *) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the kvpmem in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
int ha_kvpmem::rnd_init(bool) {
  DBUG_TRACE;

  DBUG_PRINT("KVDK", ("rnd_init:"));

  print_db();

  // seek to first element of table
  pmem::kv::status s = read_it->seek(create_key(active_table, ""));
  assert(s == pmem::kv::status::OK);

  return 0;
}

int ha_kvpmem::rnd_end() {
  DBUG_TRACE;
  return 0;
}

/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
int ha_kvpmem::rnd_next(uchar *ret) {
  DBUG_TRACE;
  DBUG_PRINT("KVDK", ("rnd_next:"));

  // if not yet inited should not happen...
  if (read_it == nullptr) {
    rnd_init(false);
  }

  if (pmem::kv::status::OK == read_it->is_next()) {
    pmem::kv::status s = read_it->next();
    assert(s == pmem::kv::status::OK);

    /* read a key */

    auto key = read_key(*read_it);
    if (key == create_key(active_table, "zzz")) {
      return HA_ERR_END_OF_FILE;
    }

    auto value = read_value(*read_it);

    memcpy(ret, value.data(), table->s->reclength);
  } else {
    return HA_ERR_END_OF_FILE;
  }
  return 0;
}

/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_kvpmem::position(const uchar *) { DBUG_TRACE; }

/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and
  sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_kvpmem::rnd_pos(uchar *, uchar *) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really
  needed. SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc and sql_update.cc
*/
int ha_kvpmem::info(uint) {
  DBUG_TRACE;
  if (stats.records < 2) stats.records = 2;
  return 0;
}

/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_kvpmem::extra(enum ha_extra_function) {
  DBUG_TRACE;
  return 0;
}

/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases
  where the optimizer realizes that all rows will be removed as a result of an
  SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_query_block_query_expression::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_query_block_query_expression::exec() in sql_union.cc.
*/
int ha_kvpmem::delete_all_rows() {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to
  understand this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_kvpmem::external_lock(THD *, int) {
  DBUG_TRACE;
  DBUG_PRINT("KVDK", ("external lock"));
  return 0;
}

/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for kvpmem, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_kvpmem::store_lock(THD *, THR_LOCK_DATA **to,
                                      enum thr_lock_type lock_type) {
  DBUG_TRACE;
  DBUG_PRINT("KVDK", ("store_lock"));
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) lock.type = lock_type;
  *to++ = &lock;
  return to;
}

/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_kvpmem::delete_table(const char *, const dd::Table *) {
  DBUG_TRACE;
  /* This is not implemented but we want someone to be able that it works. */
  return 0;
}

/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_kvpmem::rename_table(const char *, const char *, const dd::Table *,
                            dd::Table *) {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_kvpmem::records_in_range(uint, key_range *, key_range *) {
  DBUG_TRACE;
  return 10;  // low number to force index usage
}

static MYSQL_THDVAR_STR(last_create_thdvar, PLUGIN_VAR_MEMALLOC, nullptr,
                        nullptr, nullptr, nullptr);

static MYSQL_THDVAR_UINT(create_count_thdvar, 0, nullptr, nullptr, nullptr, 0,
                         0, 1000, 0);

/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_kvpmem::create(const char *table_name, TABLE *, HA_CREATE_INFO *,
                      dd::Table *) {
  DBUG_TRACE;
  DBUG_PRINT("KVDK", ("CREATE TABLE %s", table_name));

  std::string table_name_str = std::string(table_name);

  // use mutex to protect table creation since table num has to be saved to PMEM
  table_create_mutex.lock();

  insert_table_markers(&table_name_str);
  table_create_mutex.unlock();

  DBUG_PRINT("KVDK", ("CURR TABLE NUM AT CREATE"));

  return 0;
}

uint ha_kvpmem::max_supported_keys() const { return (1); }

/** Get the table flags to use for the statement.
 @return table flags */

handler::Table_flags ha_kvpmem::table_flags() const {
  handler::Table_flags flags = m_int_table_flags;

  return (flags);
}

struct st_mysql_storage_engine kvpmem_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

static ulong srv_enum_var = 0;
static ulong srv_ulong_var = 0;
static double srv_double_var = 0;
static int srv_signed_int_var = 0;
static long srv_signed_long_var = 0;
static longlong srv_signed_longlong_var = 0;

const char *enum_var_names[] = {"e1", "e2", NullS};

TYPELIB enum_var_typelib = {array_elements(enum_var_names) - 1,
                            "enum_var_typelib", enum_var_names, nullptr};

static MYSQL_SYSVAR_ENUM(enum_var,                        // name
                         srv_enum_var,                    // varname
                         PLUGIN_VAR_RQCMDARG,             // opt
                         "Sample ENUM system variable.",  // comment
                         nullptr,                         // check
                         nullptr,                         // update
                         0,                               // def
                         &enum_var_typelib);              // typelib

static MYSQL_SYSVAR_ULONG(ulong_var, srv_ulong_var, PLUGIN_VAR_RQCMDARG,
                          "0..1000", nullptr, nullptr, 8, 0, 1000, 0);

static MYSQL_SYSVAR_DOUBLE(double_var, srv_double_var, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", nullptr, nullptr, 8.5, 0.5,
                           1000.5,
                           0);  // reserved always 0

static MYSQL_THDVAR_DOUBLE(double_thdvar, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", nullptr, nullptr, 8.5, 0.5,
                           1000.5, 0);

static MYSQL_SYSVAR_INT(signed_int_var, srv_signed_int_var, PLUGIN_VAR_RQCMDARG,
                        "INT_MIN..INT_MAX", nullptr, nullptr, -10, INT_MIN,
                        INT_MAX, 0);

static MYSQL_THDVAR_INT(signed_int_thdvar, PLUGIN_VAR_RQCMDARG,
                        "INT_MIN..INT_MAX", nullptr, nullptr, -10, INT_MIN,
                        INT_MAX, 0);

static MYSQL_SYSVAR_LONG(signed_long_var, srv_signed_long_var,
                         PLUGIN_VAR_RQCMDARG, "LONG_MIN..LONG_MAX", nullptr,
                         nullptr, -10, LONG_MIN, LONG_MAX, 0);

static MYSQL_THDVAR_LONG(signed_long_thdvar, PLUGIN_VAR_RQCMDARG,
                         "LONG_MIN..LONG_MAX", nullptr, nullptr, -10, LONG_MIN,
                         LONG_MAX, 0);

static MYSQL_SYSVAR_LONGLONG(signed_longlong_var, srv_signed_longlong_var,
                             PLUGIN_VAR_RQCMDARG, "LLONG_MIN..LLONG_MAX",
                             nullptr, nullptr, -10, LLONG_MIN, LLONG_MAX, 0);

static MYSQL_THDVAR_LONGLONG(signed_longlong_thdvar, PLUGIN_VAR_RQCMDARG,
                             "LLONG_MIN..LLONG_MAX", nullptr, nullptr, -10,
                             LLONG_MIN, LLONG_MAX, 0);

static SYS_VAR *kvpmem_system_variables[] = {
    MYSQL_SYSVAR(enum_var),
    MYSQL_SYSVAR(ulong_var),
    MYSQL_SYSVAR(double_var),
    MYSQL_SYSVAR(double_thdvar),
    MYSQL_SYSVAR(last_create_thdvar),
    MYSQL_SYSVAR(create_count_thdvar),
    MYSQL_SYSVAR(signed_int_var),
    MYSQL_SYSVAR(signed_int_thdvar),
    MYSQL_SYSVAR(signed_long_var),
    MYSQL_SYSVAR(signed_long_thdvar),
    MYSQL_SYSVAR(signed_longlong_var),
    MYSQL_SYSVAR(signed_longlong_thdvar),
    nullptr};

// this is an kvpmem of SHOW_FUNC
static int show_func_kvpmem(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_CHAR;
  var->value = buf;  // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
           "enum_var is %lu, ulong_var is %lu, "
           "double_var is %f, signed_int_var is %d, "
           "signed_long_var is %ld, signed_longlong_var is %lld",
           srv_enum_var, srv_ulong_var, srv_double_var, srv_signed_int_var,
           srv_signed_long_var, srv_signed_longlong_var);
  return 0;
}

struct kvpmem_vars_t {
  ulong var1;
  double var2;
  char var3[64];
  bool var4;
  bool var5;
  ulong var6;
};

kvpmem_vars_t kvpmem_vars = {100, 20.01, "three hundred", true, false, 8250};

static SHOW_VAR show_status_kvpmem[] = {
    {"var1", (char *)&kvpmem_vars.var1, SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"var2", (char *)&kvpmem_vars.var2, SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF,
     SHOW_SCOPE_UNDEF}  // null terminator required
};

static SHOW_VAR show_array_kvpmem[] = {
    {"array", (char *)show_status_kvpmem, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
    {"var3", (char *)&kvpmem_vars.var3, SHOW_CHAR, SHOW_SCOPE_GLOBAL},
    {"var4", (char *)&kvpmem_vars.var4, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

static SHOW_VAR func_status[] = {
    {"kvpmem_func_kvpmem", (char *)show_func_kvpmem, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},
    {"kvpmem_status_var5", (char *)&kvpmem_vars.var5, SHOW_BOOL,
     SHOW_SCOPE_GLOBAL},
    {"kvpmem_status_var6", (char *)&kvpmem_vars.var6, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"kvpmem_status", (char *)show_array_kvpmem, SHOW_ARRAY,
     SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

mysql_declare_plugin(kvpmem){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &kvpmem_storage_engine,
    "KVPMEM",
    PLUGIN_AUTHOR_ORACLE,
    "KVpmem storage engine",
    PLUGIN_LICENSE_GPL,
    kvpmem_init_func, /* Plugin Init */
    nullptr,           /* Plugin check uninstall */
    nullptr,           /* Plugin Deinit */
    0x0001 /* 0.1 */,
    func_status,              /* status variables */
    kvpmem_system_variables, /* system variables */
    nullptr,                  /* config options */
    0,                        /* flags */
} mysql_declare_plugin_end;

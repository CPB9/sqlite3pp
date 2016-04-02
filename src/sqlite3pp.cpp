// sqlite3pp.cpp
//
// The MIT License
//
// Copyright (c) 2015 Wongoo Lee (iwongu at gmail dot com)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include <cstring>
#include <cassert>

#include "sqlite3.h"
#include "sqlite3pp.h"

namespace sqlite3pp
{

namespace
{
int busy_handler_impl(void* p, int cnt)
{
    auto h = static_cast<database::busy_handler*>(p);
    return (*h)(cnt);
}

int commit_hook_impl(void* p)
{
    auto h = static_cast<database::commit_handler*>(p);
    return (*h)();
}

void rollback_hook_impl(void* p)
{
    auto h = static_cast<database::rollback_handler*>(p);
    (*h)();
}

void update_hook_impl(void* p, int opcode, char const* dbname, char const* tablename, int64_t rowid)
{
    auto h = static_cast<database::update_handler*>(p);
    (*h)(opcode, dbname, tablename, rowid);
}

int authorizer_impl(void* p, int evcode, char const* p1, char const* p2, char const* dbname, char const* tvname)
{
    auto h = static_cast<database::authorize_handler*>(p);
    return (*h)(evcode, p1, p2, dbname, tvname);
}

} // namespace

template<>
int query::rows::get<int>(uint idx) const
{
    return sqlite3_column_int(stmt_, idx);
}

template<>
int64_t query::rows::get<int64_t>(uint idx) const
{
    return sqlite3_column_int64(stmt_, idx);
}

template<>
double query::rows::get<double>(uint idx) const
{
    return sqlite3_column_double(stmt_, idx);
}

template<>
const char* query::rows::get<const char*>(uint idx) const
{
    return reinterpret_cast<const char*>(sqlite3_column_text(stmt_, idx));
}

template<>
std::string query::rows::get<std::string>(uint idx) const
{
    return get<const char*>(idx);
}

template<>
bmcl::Bytes query::rows::get<bmcl::Bytes>(uint idx) const
{
    const void* p = sqlite3_column_blob(stmt_, idx);
    int size = sqlite3_column_bytes(stmt_, idx);
    return bmcl::Bytes(reinterpret_cast<const uint8_t*>(p), size);
}

std::string to_string(Error err)
{
    return sqlite3_errstr(err);
}

database::database() : db_(nullptr)
{
}

database::database(bmcl::StringView db, uint flags, bmcl::StringView vfs) : db_(nullptr)
{
    if (db.isEmpty())
        return;

    auto r = connect(db.data(), flags, vfs.data());
    if (r.isSome())
    {
        assert(false);
    }
}

database::database(database&& db)
    : db_(std::move(db.db_))
    , bh_(std::move(db.bh_))
    , ch_(std::move(db.ch_))
    , rh_(std::move(db.rh_))
    , uh_(std::move(db.uh_))
    , ah_(std::move(db.ah_))
{
    db.db_ = nullptr;
}

database& database::operator=(database&& db)
{
    db_ = std::move(db.db_);
    db.db_ = nullptr;

    bh_ = std::move(db.bh_);
    ch_ = std::move(db.ch_);
    rh_ = std::move(db.rh_);
    uh_ = std::move(db.uh_);
    ah_ = std::move(db.ah_);

    return *this;
}

database::~database()
{
    disconnect();
}

static inline bmcl::Option<Error> sqlite_call(int r)
{
    if (r == SQLITE_OK)
        return bmcl::None;
    return r;
}

bmcl::Option<Error> database::connect(bmcl::StringView dbname, uint flags, bmcl::StringView vfs)
{
    disconnect();
    assert(!dbname.isEmpty());
    return sqlite_call(sqlite3_open_v2(dbname.data(), &db_, flags, vfs.data()));
}

bmcl::Option<Error> database::disconnect()
{
    if (!db_)
        return bmcl::None;

    auto r = sqlite_call(sqlite3_close(db_));
    if (r.isNone())
        db_ = nullptr;
    return r;
}

bmcl::Option<Error> database::attach(bmcl::StringView db, bmcl::StringView name)
{
    return executef("ATTACH '%q' AS '%q'", db.data(), name.data());
}

bmcl::Option<Error> database::detach(bmcl::StringView name)
{
    return executef("DETACH '%q'", name.data());
}

void database::set_busy_handler(busy_handler h)
{
    bh_ = h;
    sqlite3_busy_handler(db_, bh_ ? busy_handler_impl : 0, &bh_);
}

void database::set_commit_handler(commit_handler h)
{
    ch_ = h;
    sqlite3_commit_hook(db_, ch_ ? commit_hook_impl : 0, &ch_);
}

void database::set_rollback_handler(rollback_handler h)
{
    rh_ = h;
    sqlite3_rollback_hook(db_, rh_ ? rollback_hook_impl : 0, &rh_);
}

void database::set_update_handler(update_handler h)
{
    uh_ = h;
    sqlite3_update_hook(db_, uh_ ? update_hook_impl : 0, &uh_);
}

void database::set_authorize_handler(authorize_handler h)
{
    ah_ = h;
    sqlite3_set_authorizer(db_, ah_ ? authorizer_impl : 0, &ah_);
}

bmcl::Option<uint64_t> database::last_insert_rowid() const
{
    int64_t id = sqlite3_last_insert_rowid(db_);
    if (id > 0)
        return id;
    return bmcl::None;
}

bmcl::Option<Error> database::enable_foreign_keys(bool enable)
{
    return sqlite_call(sqlite3_db_config(db_, SQLITE_DBCONFIG_ENABLE_FKEY, enable ? 1 : 0, nullptr));
}

bmcl::Option<Error> database::enable_triggers(bool enable)
{
    return sqlite_call(sqlite3_db_config(db_, SQLITE_DBCONFIG_ENABLE_TRIGGER, enable ? 1 : 0, nullptr));
}

bmcl::Option<Error> database::enable_extended_result_codes(bool enable)
{
    return sqlite_call(sqlite3_extended_result_codes(db_, enable ? 1 : 0));
}

Error database::error_code() const
{
    return sqlite3_errcode(db_);
}

char const* database::error_msg() const
{
    return sqlite3_errmsg(db_);
}

bmcl::Option<Error> database::execute(bmcl::StringView sql)
{
    return sqlite_call(sqlite3_exec(db_, sql.data(), 0, 0, 0));
}

bmcl::Option<Error> database::executef(bmcl::StringView sql, ...)
{
    va_list ap;
    va_start(ap, sql);
    auto ptr = sqlite3_vmprintf(sql.data(), ap);
    assert(ptr);
    va_end(ap);
    auto r = execute(ptr);
    sqlite3_free(ptr);
    return r;
}

bmcl::Option<Error> database::set_busy_timeout(std::chrono::milliseconds timeout)
{
    return sqlite_call(sqlite3_busy_timeout(db_, static_cast<int>(timeout.count())));
}

statement::statement(database& db, bmcl::StringView stmt) : db_(db), stmt_(nullptr), tail_(nullptr)
{
    if (stmt.isEmpty())
        return;
    auto r = prepare(stmt);
    if (r.isSome())
    {
        assert(false);
        throw database_error(db_);
    }
}

statement::~statement()
{
    auto r = finish();
    if (r.isSome())
    {
        assert(false);
    }
}

bmcl::Option<Error> statement::prepare(bmcl::StringView stmt)
{
    auto r = finish();
    if (r.isSome())
        return r;
    return prepare_impl(stmt);
}

bmcl::Option<Error> statement::prepare_impl(bmcl::StringView stmt)
{
    return sqlite_call(sqlite3_prepare(db_.db_, stmt.data(), static_cast<int>(stmt.size()), &stmt_, &tail_));
}

bmcl::Option<Error> statement::finish()
{
    if (!stmt_)
        return bmcl::None;

    auto rc = finish_impl(stmt_);
    stmt_ = nullptr;
    tail_ = nullptr;
    return rc;
}

bmcl::Option<Error> statement::finish_impl(sqlite3_stmt* stmt)
{
    return sqlite_call(sqlite3_finalize(stmt));
}

bmcl::Result<bool, Error> statement::step()
{
    auto r = sqlite3_step(stmt_);
    if (r == SQLITE_DONE)
        return false;
    else if (r == SQLITE_ROW)
        return true;
    assert(r != SQLITE_OK);
    return r;
}

bmcl::Option<Error> statement::reset()
{
    return sqlite_call(sqlite3_reset(stmt_));
}

bmcl::Option<Error> statement::bind(uint idx, int value)
{
    return sqlite_call(sqlite3_bind_int(stmt_, idx, value));
}

bmcl::Option<Error> statement::bind(uint idx, double value)
{
    return sqlite_call(sqlite3_bind_double(stmt_, idx, value));
}

bmcl::Option<Error> statement::bind(uint idx, int64_t value)
{
    return sqlite_call(sqlite3_bind_int64(stmt_, idx, value));
}

bmcl::Option<Error> statement::bind(uint idx, bmcl::StringView value, copy_semantic fcopy)
{
    return sqlite_call(sqlite3_bind_text(stmt_, idx, value.data(), static_cast<int>(value.size()), fcopy == copy ? SQLITE_TRANSIENT : SQLITE_STATIC ));
}

bmcl::Option<Error> statement::bind(uint idx, bmcl::Bytes value, copy_semantic fcopy)
{
    return sqlite_call(sqlite3_bind_blob(stmt_, idx, value.data(), value.size(), fcopy == copy ? SQLITE_TRANSIENT : SQLITE_STATIC ));
}

bmcl::Option<Error> statement::bind(uint idx, null_type)
{
    return bind(idx);
}

bmcl::Option<Error> statement::bind(uint idx)
{
    return sqlite_call(sqlite3_bind_null(stmt_, idx));
}

bmcl::Option<Error> statement::bind(bmcl::StringView name, int value)
{
    auto idx = sqlite3_bind_parameter_index(stmt_, name.data());
    return bind(idx, value);
}

bmcl::Option<Error> statement::bind(bmcl::StringView name, double value)
{
    auto idx = sqlite3_bind_parameter_index(stmt_, name.data());
    return bind(idx, value);
}

bmcl::Option<Error> statement::bind(bmcl::StringView name, int64_t value)
{
    auto idx = sqlite3_bind_parameter_index(stmt_, name.data());
    return bind(idx, value);
}

bmcl::Option<Error> statement::bind(bmcl::StringView name, bmcl::StringView value, copy_semantic fcopy)
{
    auto idx = sqlite3_bind_parameter_index(stmt_, name.data());
    return bind(idx, value, fcopy);
}

bmcl::Option<Error> statement::bind(bmcl::StringView name, bmcl::Bytes value, copy_semantic fcopy)
{
    auto idx = sqlite3_bind_parameter_index(stmt_, name.data());
    return bind(idx, value, fcopy);
}

bmcl::Option<Error> statement::bind(bmcl::StringView name, null_type)
{
    return bind(name);
}

bmcl::Option<Error> statement::bind(bmcl::StringView name)
{
    auto idx = sqlite3_bind_parameter_index(stmt_, name.data());
    return bind(idx);
}

command::bindstream::bindstream(command& cmd, uint idx) : cmd_(cmd), idx_(idx)
{
}

command::bindstream& command::bindstream::operator<< (bmcl::StringView value)
{
    auto r = cmd_.bind(idx_, value, copy);
    if (r.isSome())
    {
        assert(false);
        throw database_error(cmd_.db_);
    }
    ++idx_;
    return *this;
}

command::command(database& db, bmcl::StringView stmt) : statement(db, stmt)
{
}

command::bindstream command::binder(uint idx)
{
    return bindstream(*this, idx);
}

bmcl::Result<bool, Error> command::execute()
{
    return step();
}

bmcl::Option<Error> command::execute_all()
{
    auto rc = execute();
    if (rc.isErr())
        return rc.unwrapErr();

    const char* sql = tail_;

    while (std::strlen(sql) > 0)
    { // sqlite3_complete() is broken.
        sqlite3_stmt* old_stmt = stmt_;
        auto r = prepare_impl(sql);
        if (r.isSome())
            return r;

        r = sqlite_call(sqlite3_transfer_bindings(old_stmt, stmt_));
        if (r.isSome())
            return r;

        r = finish_impl(old_stmt);
        if (r.isSome())
            return r;

        rc = execute();
        if (rc.isErr())
            return rc.unwrapErr();

        sql = tail_;
    }

    return bmcl::None;
}


query::rows::getstream::getstream(rows* rws, uint idx) : rws_(rws), idx_(idx)
{
}

query::rows::rows(sqlite3_stmt* stmt) : stmt_(stmt)
{
}

uint query::rows::data_count() const
{
    return static_cast<uint>(sqlite3_data_count(stmt_));
}

data_type query::rows::column_type(uint idx) const
{
    return static_cast<data_type>(sqlite3_column_type(stmt_, idx));
}

uint query::rows::column_bytes(uint idx) const
{
    return static_cast<uint>(sqlite3_column_bytes(stmt_, idx));
}

query::rows::getstream query::rows::getter(uint idx)
{
    return getstream(this, idx);
}

query::query_iterator::query_iterator() : cmd_(nullptr), isDone_(true)
{
}

query::query_iterator::query_iterator(query* cmd) : cmd_(cmd)
{
    auto r = cmd_->step();
    if (r.isErr())
    {
        isDone_ = true;
        rc_ = r.unwrapErr();
        //throw database_error(cmd_->db_);
        return;
    }
    isDone_ = r.unwrap();
}

bool query::query_iterator::operator==(query::query_iterator const& other) const
{
    return isDone_ == other.isDone_;
}

bool query::query_iterator::operator!=(query::query_iterator const& other) const
{
    return isDone_ != other.isDone_;
}

query::query_iterator& query::query_iterator::operator++()
{
    auto r = cmd_->step();
    if (r.isErr())
    {
        isDone_ = true;
        rc_ = r.unwrapErr();
        //throw database_error(cmd_->db_);
        return *this;
    }
    isDone_ = r.unwrap();
    return *this;
}

query::query_iterator::value_type query::query_iterator::operator*() const
{
    return rows(cmd_->stmt_);
}

query::query(database& db, bmcl::StringView stmt) : statement(db, stmt)
{
}

uint query::column_count() const
{
    return sqlite3_column_count(stmt_);
}

char const* query::column_name(uint idx) const
{
    return sqlite3_column_name(stmt_, idx);
}

char const* query::column_decltype(uint idx) const
{
    return sqlite3_column_decltype(stmt_, idx);
}


query::iterator query::begin()
{
    return query_iterator(this);
}

query::iterator query::end()
{
    return query_iterator();
}


transaction::transaction(database& db, bool fcommit, bool freserve) : db_(&db), fcommit_(fcommit)
{
    auto rc = db_->execute(freserve ? "BEGIN IMMEDIATE" : "BEGIN");
    if (rc.isSome())
    {
        db_ = nullptr;
        assert(false);
    }
}

transaction::~transaction() noexcept(false)
{
    if (!db_)
        return;

    auto r = db_->execute(fcommit_ ? "COMMIT" : "ROLLBACK");
    if (r.isSome())
    {
        assert(false);
    }
}

bmcl::Option<Error> transaction::commit()
{
    auto db = db_;
    db_ = nullptr;
    return db->execute("COMMIT");
}

bmcl::Option<Error> transaction::rollback()
{
    auto db = db_;
    db_ = nullptr;
    return db->execute("ROLLBACK");
}


database_error::database_error(bmcl::StringView msg) : std::runtime_error(msg.data())
{
}

database_error::database_error(database& db) : std::runtime_error(sqlite3_errmsg(db.db_))
{
}

} // namespace sqlite3pp

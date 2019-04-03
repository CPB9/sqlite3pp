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
#include <bmcl/Logging.h>
#include <bmcl/Panic.h>
#include <bmcl/Assert.h>
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

void update_hook_impl(void* p, int opcode, char const* dbname, char const* tablename, long long int rowid)
{
    auto h = static_cast<database::update_handler*>(p);
    (*h)(opcode, dbname, tablename, rowid);
}

int authorizer_impl(void* p, int evcode, char const* p1, char const* p2, char const* dbname, char const* tvname)
{
    auto h = static_cast<database::authorize_handler*>(p);
    return (*h)(evcode, p1, p2, dbname, tvname);
}

void error_logger_impl(void* p, int err, char const* msg)
{
    auto h = static_cast<database::error_handler*>(p);
    (*h)((sqlite3pp::Error)err, msg);
}

} // namespace

sql_str::sql_str(sql_str&& other) { _p = other._p; other._p = nullptr; }
sql_str::~sql_str() { if (_p) sqlite3_free((void*)_p); }
const char* sql_str::as_is() const { return _p; }
const char* sql_str::non_null() const { return _p ? _p : ""; }
std::string sql_str::to_string() const { return std::string(_p); }
sql_str::sql_str(const char* p) : _p(p) {}

template<>
bool selecter::row::get<bool>(uint idx) const
{
    return sqlite3_column_int(stmt_->stmt_, idx);
}

template<>
int selecter::row::get<int>(uint idx) const
{
    return sqlite3_column_int(stmt_->stmt_, idx);
}

template<>
int64_t selecter::row::get<int64_t>(uint idx) const
{
    return sqlite3_column_int64(stmt_->stmt_, idx);
}

template<>
double selecter::row::get<double>(uint idx) const
{
    return sqlite3_column_double(stmt_->stmt_, idx);
}

template<>
bmcl::StringView selecter::row::get<bmcl::StringView>(uint idx) const
{
    const void* p = sqlite3_column_text(stmt_->stmt_, idx);
    int size = sqlite3_column_bytes(stmt_->stmt_, idx);
    return bmcl::StringView(reinterpret_cast<const char*>(p), size);
}

template<>
bmcl::Bytes selecter::row::get<bmcl::Bytes>(uint idx) const
{
    const void* p = sqlite3_column_blob(stmt_->stmt_, idx);
    int size = sqlite3_column_bytes(stmt_->stmt_, idx);
    return bmcl::Bytes(reinterpret_cast<const uint8_t*>(p), size);
}

const char* to_string(Error err)
{
    return sqlite3_errstr(static_cast<int>(err));
}

database::error_handler database::eh_;

database::database() : db_(nullptr)
{
}

database::database(const char* dbname, uint flags, const char* vfs)
{
    auto r = connect(dbname, flags, vfs);
    if (r.isSome())
    {
        assert(false);
    }
}

database::database(const std::string& dbname, uint flags, const std::string& vfs)
{
    auto r = connect(dbname, flags, vfs);
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

bool database::is_connected() const
{
    return db_ != nullptr;
}

static inline OptError sqlite_call(int r)
{
    if (r == SQLITE_OK)
        return bmcl::None;
    return static_cast<Error>(r);
}

OptError database::connect(const char* dbname, uint flags, const char* vfs)
{
    disconnect();
    assert(dbname);
    return sqlite_call(sqlite3_open_v2(dbname, &db_, flags, vfs));
}

OptError database::connect(const std::string& dbname, uint flags, const std::string& vfs)
{
    return connect(dbname.empty() ? nullptr : dbname.c_str(), flags, vfs.empty() ? nullptr : vfs.c_str());
}

OptError database::disconnect()
{
    if (!db_)
        return bmcl::None;

    auto r = sqlite_call(sqlite3_close(db_));
    if (r.isNone())
    {
        db_ = nullptr;
    }
    else
    {
        BMCL_CRITICAL() << "Sqlite error: " << to_string(*r);
    }
    return r;
}

OptError database::attach(const char* db, const char* name)
{
    return executef("ATTACH '%q' AS '%q'", db, name);
}

OptError database::attach(const std::string& db, const std::string& name)
{
    return attach(db.empty() ? nullptr : db.c_str(), name.empty() ? nullptr : name.c_str());
}

OptError database::detach(const char* name)
{
    return executef("DETACH '%q'", name);
}

OptError database::detach(const std::string& name)
{
    return detach(name.empty() ? nullptr : name.c_str());
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

void database::set_error_handler(error_handler h)
{
    eh_ = h;
    sqlite3_config(SQLITE_CONFIG_LOG, eh_ ? error_logger_impl : 0, &eh_);
}

bmcl::Option<int64_t> database::last_insert_rowid() const
{
    int64_t id = sqlite3_last_insert_rowid(db_);
    if (id > 0)
        return id;
    return bmcl::None;
}

OptError database::synchronous_mode(synchronous value)
{
    switch (value)
    {
    case sqlite3pp::synchronous::Off:   return executef("PRAGMA synchronous = OFF");
    case sqlite3pp::synchronous::Normal:return executef("PRAGMA synchronous = NORMAL");
    case sqlite3pp::synchronous::Full:  return executef("PRAGMA synchronous = FULL");
    case sqlite3pp::synchronous::Extra: return executef("PRAGMA synchronous = EXTRA");
    };
    return static_cast<Error>(SQLITE_ERROR);
}

OptError database::enable_foreign_keys(bool enable)
{
    return sqlite_call(sqlite3_db_config(db_, SQLITE_DBCONFIG_ENABLE_FKEY, enable ? 1 : 0, nullptr));
}

OptError database::enable_triggers(bool enable)
{
    return sqlite_call(sqlite3_db_config(db_, SQLITE_DBCONFIG_ENABLE_TRIGGER, enable ? 1 : 0, nullptr));
}

OptError database::enable_extended_result_codes(bool enable)
{
    return sqlite_call(sqlite3_extended_result_codes(db_, enable ? 1 : 0));
}

const char* database::version()
{
    return SQLITE_VERSION;
}

bool database::is_threadsafe()
{
    return sqlite3_threadsafe();
}

const char* database::filename()
{
    return sqlite3_db_filename(db_, nullptr);
}

OptError database::execute(const char* sql)
{
    return sqlite_call(sqlite3_exec(db_, sql, 0, 0, 0));
}

OptError database::execute(const std::string& sql)
{
    return execute(sql.c_str());
}

OptError database::executef(const char* sql, ...)
{
    va_list ap;
    va_start(ap, sql);
    auto ptr = sqlite3_vmprintf(sql, ap);
    assert(ptr);
    va_end(ap);
    auto r = execute(ptr);
    sqlite3_free(ptr);
    return r;
}

OptError database::set_busy_timeout(std::chrono::milliseconds timeout)
{
    return sqlite_call(sqlite3_busy_timeout(db_, static_cast<int>(timeout.count())));
}

OptError database::begin(bool immediate)
{
    return immediate ? execute("BEGIN IMMEDIATE") : execute("BEGIN");
}

OptError database::commit()
{
    return execute("COMMIT");
}

OptError database::rollback()
{
    return execute("ROLLBACK");
}

statement::statement(database& db, bmcl::StringView stmt) : db_(db), stmt_(nullptr)
{
    if (stmt.isEmpty())
        return;
    auto r = prepare(stmt, nullptr);
    if (r.isSome())
    {
        std::string e = std::string("Sqlite error: ") + to_string(r.unwrap());
        BMCL_CRITICAL() << e;
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

database& statement::db()
{
    return db_;
}

const database& statement::db() const
{
    return db_;
}

OptError statement::prepare(bmcl::StringView stmt, bmcl::StringView* left)
{
    auto r = finish();
    if (r.isSome())
    {
        if (left) *left = stmt;
        return r;
    }

    const char* tail = nullptr;
    r = sqlite_call(sqlite3_prepare_v2(db_.db_, stmt.data(), static_cast<int>(stmt.size()), &stmt_, &tail));
    if (left)
        *left = bmcl::StringView(tail, stmt.end());

    return r;
}

bmcl::Option<const char*> database::err_msg() const
{
    const char* p = sqlite3_errmsg(db_);
    if (p) return p;
    return bmcl::None;
}

bmcl::Option<int64_t> database::changes() const
{
    int64_t count = sqlite3_changes(db_);
    if (count > 0)
        return count;
    return bmcl::None;
}

bmcl::Option<const char*> statement::err_msg() const
{
    return db_.err_msg();
}

OptError statement::finish()
{
    if (!stmt_)
        return bmcl::None;

    sqlite3_reset(stmt_);
    auto r = sqlite_call(sqlite3_finalize(stmt_));
    if (r.isSome())
    {
        assert(r.isNone());
    }
    stmt_ = nullptr;
    return r;
}

bmcl::Result<bool, Error> statement::step()
{
    auto r = sqlite3_step(stmt_);
    if (r == SQLITE_DONE)
        return false;
    else if (r == SQLITE_ROW)
        return true;
    return static_cast<Error>(r);
}

OptError statement::exec()
{
    if (!stmt_)
        return static_cast<Error>(SQLITE_MISUSE);

    {
        auto r = reset();
        if (r.isSome())
            return r;
    }

    auto r = sqlite3_step(stmt_);
    if (r == SQLITE_DONE || r == SQLITE_ROW || r == SQLITE_OK)
        return bmcl::None;
    return static_cast<Error>(r);
}

sql_str statement::sql() const
{
    const char* p = sqlite3_expanded_sql(stmt_);
    if (p == nullptr)
        return sql_str(nullptr);
    return sql_str(p);
}

OptError statement::reset()
{
    return sqlite_call(sqlite3_reset(stmt_));
}

OptError statement::clear_bindings()
{
    return sqlite_call(sqlite3_clear_bindings(stmt_));
}

OptError statement::bind(uint idx, std::nullptr_t)
{
    return sqlite_call(sqlite3_bind_null(stmt_, idx));
}

OptError statement::bind(uint idx, bool value)
{
    return sqlite_call(sqlite3_bind_int(stmt_, idx, value));
}

OptError statement::bind(uint idx, int value)
{
    return sqlite_call(sqlite3_bind_int(stmt_, idx, value));
}

OptError statement::bind(uint idx, double value)
{
    return sqlite_call(sqlite3_bind_double(stmt_, idx, value));
}

OptError statement::bind(uint idx, int64_t value)
{
    return sqlite_call(sqlite3_bind_int64(stmt_, idx, value));
}

OptError statement::bind(uint idx, bmcl::StringView value, copy_semantic fcopy)
{
    return sqlite_call(sqlite3_bind_text(stmt_, idx, value.data(), static_cast<int>(value.size()), fcopy == copy ? SQLITE_TRANSIENT : SQLITE_STATIC));
}

OptError statement::bind(uint idx, bmcl::Bytes value, copy_semantic fcopy)
{
    return sqlite_call(sqlite3_bind_blob(stmt_, idx, value.data(), static_cast<int>(value.size()), fcopy == copy ? SQLITE_TRANSIENT : SQLITE_STATIC));
}

OptError statement::bind(uint idx, bmcl::Option<bool> value)
{
    if (value.isNone()) return bind(idx, nullptr);
    return bind(idx, *value);
}

OptError statement::bind(uint idx, bmcl::Option<int> value)
{
    if (value.isNone()) return bind(idx, nullptr);
    return bind(idx, *value);
}

OptError statement::bind(uint idx, bmcl::Option<double> value)
{
    if (value.isNone()) return bind(idx, nullptr);
    return bind(idx, *value);
}

OptError statement::bind(uint idx, bmcl::Option<int64_t> value)
{
    if (value.isNone()) return bind(idx, nullptr);
    return bind(idx, *value);
}

OptError statement::bind(uint idx, bmcl::Option<bmcl::StringView> value, copy_semantic fcopy)
{
    if (value.isNone()) return bind(idx, nullptr);
    return bind(idx, *value, fcopy);
}

OptError statement::bind(uint idx, bmcl::Option<bmcl::Bytes> value, copy_semantic fcopy)
{
    if (value.isNone()) return bind(idx, nullptr);
    return bind(idx, *value, fcopy);
}

bmcl::Result<uint, Error> statement::bind_index(const char* name)
{
    uint r = sqlite3_bind_parameter_index(stmt_, name);
    if (r == 0)
        return static_cast<Error>(SQLITE_MISUSE);
    return r;
}

statement::bindstream::bindstream(statement& stmt, uint idx) : stmt_(stmt), idx_(idx)
{
}

statement::bindstream& statement::bindstream::operator<< (bmcl::StringView value)
{
    auto r = stmt_.bind(idx_, value, copy);
    if (r.isSome())
    {
        assert(false);
        throw database_error(stmt_.db_);
    }
    ++idx_;
    return *this;
}

statement::bindstream statement::binder(uint idx)
{
    return bindstream(*this, idx);
}

batch::batch(database& db) : db_(db), state_(nullptr), orig_(nullptr)
{
}

batch::batch(database& db, bmcl::StringView stmt, copy_semantic fcopy) : batch(db)
{
    auto r = prepare(stmt, fcopy);
}

void batch::reset()
{
    state_ = orig_;
}

OptError batch::prepare(bmcl::StringView stmt, copy_semantic fcopy)
{
    if (!stmt.isEmpty() && fcopy)
    {
        data_ = stmt.toStdString();
        orig_ = data_.unwrap();
    }
    else
    {
        orig_ = stmt;
    }
    state_ = orig_;
    return bmcl::None;
}

bmcl::Result<bool, Error> batch::execute_next()
{
    state_ = state_.ltrim();
    if (state_.isEmpty())
        return false;

    statement stmt(db_);
    bmcl::StringView view = state_;

    {
        auto r = stmt.prepare(state_, &view);
        if (r.isSome())
            return r.unwrap();
    }

    {
        auto r = stmt.step();
        if (r.isErr())
            return r.unwrapErr();
    }

    {
        auto r = stmt.finish();
        if (r.isSome())
            return r.unwrap();
    }

    state_ = view;
    return !state_.isEmpty();
}

OptError batch::execute_all()
{
    bool hasSmth;
    do
    {
        auto r = execute_next();
        if (r.isErr())
            return r.unwrapErr();
        hasSmth = r.unwrap();
    } while (hasSmth);
    return bmcl::None;
}

bmcl::StringView batch::state() const
{
    return state_;
}

selecter::row::getstream::getstream(row* rws, uint idx) : rw_(rws), idx_(idx)
{
}

selecter::row::row(selecter* stmt) : stmt_(stmt)
{
}

uint selecter::row::count() const
{
    return static_cast<uint>(sqlite3_data_count(stmt_->stmt_));
}

data_type selecter::row::type(uint idx) const
{
    return static_cast<data_type>(sqlite3_column_type(stmt_->stmt_, idx));
}

bool selecter::row::is_null(uint idx) const
{
    return type(idx) == data_type::Null;
}

bool selecter::row::is_null(bmcl::StringView name) const
{
    return type(name) == data_type::Null;
}

data_type selecter::row::type(bmcl::StringView name) const
{
    auto r = stmt_->column_index(name);
    if (r.isNone())
    {
        assert(false);
        return data_type::Null;
    }
    return type(*r);
}

uint selecter::row::bytes(uint idx) const
{
    return static_cast<uint>(sqlite3_column_bytes(stmt_->stmt_, idx));
}

uint selecter::row::bytes(bmcl::StringView name) const
{
    auto r = stmt_->column_index(name);
    if (r.isNone())
    {
        assert(false);
        return 0;
    }
    return bytes(*r);
}

selecter::row::getstream selecter::row::getter(uint idx)
{
    return getstream(this, idx);
}

OptError selecter::exec()
{   //overload is needed for the case where one can miss first row
    // after statement.exec, selecter.next command sequence
    if (!stmt_)
        return static_cast<Error>(SQLITE_MISUSE);

    return reset();
}

bool selecter::next()
{
    auto r = step();
    if (r.isErr())
        return false;
    return r.unwrap();
}

selecter::row selecter::get_row()
{
    return row(this);
}

selecter::selecter(database& db, bmcl::StringView stmt) : statement(db, stmt)
{
}

selecter::~selecter()
{
}

uint selecter::column_count() const
{
    return sqlite3_column_count(stmt_);
}

bmcl::StringView selecter::column_name(uint idx) const
{
    return sqlite3_column_name(stmt_, idx);
}

bmcl::StringView selecter::column_decltype(uint idx) const
{
    return sqlite3_column_decltype(stmt_, idx);
}

bmcl::Option<uint> selecter::column_index(bmcl::StringView name) const
{
    auto size = column_count();
    uint i = 0;
    while (i < size)
    {
        if (name == column_name(i))
            return i;
        ++i;
    }
    return bmcl::None;
}

inserter::inserter(database& db, bmcl::StringView stmt) : statement(db, stmt)
{
}

inserter::~inserter()
{
}

InsError inserter::insert()
{
    auto r = step();
    if (r.isErr())
        return r.unwrapErr();
    auto id = db_.last_insert_rowid();
    if (id.isSome())
        return id.unwrap();
    return static_cast<Error>(SQLITE_MISUSE);
}

transaction::transaction(database& db, bool fcommit, bool immediate) : db_(&db), fcommit_(fcommit)
{
    auto rc = db_->begin(immediate);
    if (rc.isSome())
    {
        db_ = nullptr;
        assert(false);
    }
}

transaction::~transaction()
{
    if (!db_)
        return;

    auto r = (fcommit_ ? db_->commit() : db_->rollback());
    if (r.isSome())
    {
        assert(false);
    }
}

OptError transaction::commit()
{
    if (!db_)
        return static_cast<Error>(SQLITE_MISUSE);
    auto db = db_;
    db_ = nullptr;
    return db->commit();
}

OptError transaction::rollback()
{
    if (!db_)
        return static_cast<Error>(SQLITE_MISUSE);
    auto db = db_;
    db_ = nullptr;
    return db->rollback();
}

database_error::database_error(bmcl::StringView msg) : std::runtime_error(msg.toStdString())
{
}

database_error::database_error(database& db) : std::runtime_error(sqlite3_errmsg(db.db_))
{
}

} // namespace sqlite3pp

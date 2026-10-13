from collections import defaultdict

from psycopg2 import sql
from psycopg2.extensions import cursor, connection

# list schema versions in ascending order
SCHEMA_VERSION_KEY = 'version'
SCHEMA_VERSIONS = ['v5']
# use ... (Ellipsis) here to indicate that no default value exists (will lead to error if no value is given)
ACCOUNT_DEFAULTS = {'subject': ..., 'api_key': ..., 'roles': ..., 'group': None}
PUBLIC_KEY_DEFAULTS = {'public_key': ..., 'public_key_type': ..., 'public_key_name': ...}


class SchemaVersionError(Exception):
    pass


def sanitize_record(record, defaults, **kwargs):
    sanitized = {key: record.get(key, value) for key, value in defaults.items()}
    sanitized.update(**kwargs)
    return sanitized


def make_where_clause(*filter_clauses):
    """join args of type sql.Composable via AND, dropping None, and prepend WHERE if appropriate"""
    clause = sql.SQL(' AND ').join(filter(None, filter_clauses))
    return sql.SQL(' WHERE {} ').format(clause) if clause.seq else sql.SQL('')


def db_find_account(cur: cursor, subject):
    cur.execute('''
    SELECT roles
    FROM account
    WHERE subject = %s;''', (subject, ))
    if not cur.rowcount:
        raise KeyError(subject)
    roles, = cur.fetchone()
    return roles


def db_get_apikeys(cur: cursor, subject):
    cur.execute('''
    SELECT apikeyhash
    FROM apikey
    NATURAL JOIN account
    WHERE subject = %s;''', (subject, ))
    return [row[0] for row in cur.fetchall()]


def db_get_keys(cur: cursor, subject):
    cur.execute('''
    SELECT keytype, key
    FROM publickey
    NATURAL JOIN account
    WHERE subject = %s;''', (subject, ))
    return cur.fetchall()


def db_ensure_schema_common(cur: cursor):
    # strive to make column names unique across tables so that selects become simple, such as:
    # select * from "check" natural join standardentry natural join version natural join scope;
    cur.execute('''
    CREATE TABLE IF NOT EXISTS account (
        accountid SERIAL PRIMARY KEY,
        subject text UNIQUE,
        roles integer,
        "group" text
    );
    CREATE TABLE IF NOT EXISTS delegation (
        delegateid integer NOT NULL REFERENCES account ON DELETE CASCADE ON UPDATE CASCADE,
        accountid integer NOT NULL REFERENCES account ON DELETE CASCADE ON UPDATE CASCADE,
        UNIQUE (delegateid, accountid)
    );
    CREATE TABLE IF NOT EXISTS apikey (
        apikeyid SERIAL PRIMARY KEY,
        apikeyhash text,
        accountid integer NOT NULL REFERENCES account ON DELETE CASCADE ON UPDATE CASCADE,
        UNIQUE (accountid, apikeyhash)
    );
    CREATE TABLE IF NOT EXISTS publickey (
        keyid SERIAL PRIMARY KEY,
        key text,
        keytype text,
        keyname text,
        accountid integer NOT NULL REFERENCES account ON DELETE CASCADE ON UPDATE CASCADE,
        UNIQUE (accountid, keyname)
    );
    CREATE TABLE IF NOT EXISTS report (
        reportid SERIAL PRIMARY KEY,
        reportuuid text UNIQUE,
        checked_at timestamp,
        subject text,
        data jsonb
    );
    ''')


def db_ensure_schema_v4(cur: cursor):
    db_ensure_schema_common(cur)
    cur.execute('''
    CREATE TABLE IF NOT EXISTS result2 (
        resultid SERIAL PRIMARY KEY,
        checked_at timestamp NOT NULL,
        subject text NOT NULL,
        testcase text NOT NULL,
        result int,
        reportid integer NOT NULL REFERENCES report ON DELETE CASCADE ON UPDATE CASCADE
    );
    ''')


def db_ensure_schema_v5(cur: cursor):
    db_ensure_schema_common(cur)
    cur.execute('''
    CREATE TABLE IF NOT EXISTS attestation (
        resultid SERIAL PRIMARY KEY,
        guid text NOT NULL,
        checked_at timestamp NOT NULL,
        subject text NOT NULL,
        predicate text NOT NULL,  -- such as pass, dnf, fail
        object text NOT NULL,     -- testcase id
        reportid integer NOT NULL REFERENCES report ON DELETE CASCADE ON UPDATE CASCADE
    );
    ''')


def db_upgrade_data_v4_v5(cur):
    # move stuff over from result2 to attestation
    # delete here for transaction safety
    cur.execute('''
    INSERT INTO attestation (guid, checked_at, subject, predicate, object, reportid)
    SELECT
        -- these testcases don't have a GUID yet, but the following is unique
        -- because reports at the time pertained to one subject only
        reportuuid || '.' || testcase,
        checked_at,
        subject,
        CASE result
          WHEN 1 THEN 'pass'
          WHEN 0 THEN 'dnf'
          ELSE 'fail'
        END,
        testcase,
        reportid
    FROM result2
    NATURAL JOIN report
    ;
    DELETE FROM result2
    ;''')


def db_post_upgrade_v4_v5(cur: cursor):
    cur.execute('''
    DROP TABLE IF EXISTS result2;
    ''')


def db_get_schema_version(cur: cursor):
    cur.execute('''SELECT value FROM meta WHERE key = %s;''', (SCHEMA_VERSION_KEY, ))
    return cur.rowcount and cur.fetchone()[0] or None


def db_set_schema_version(cur: cursor, version: str):
    cur.execute('''
    INSERT INTO meta (key, value)
    VALUES (%s, %s)
    ON CONFLICT (key)
    DO UPDATE
    SET value = EXCLUDED.value
    ;''', (SCHEMA_VERSION_KEY, version))


def db_upgrade_schema(conn: connection, cur: cursor):
    # the ensure_* and post_upgrade_* functions must be idempotent
    # ditto for the data transfer (ideally insert/delete transaction)
    # -- then, in case we get interrupted when setting the new version, the upgrade can be repeated
    # -- addendum: DDL is transactional with Postgres, so this effort was a bit in vain, but I keep it
    # that way just in case we want to use another database at some point
    while True:
        current = db_get_schema_version(cur)
        if current is None:
            # this is an empty db
            db_ensure_schema_v5(cur)
            db_set_schema_version(cur, 'v5')
            conn.commit()
            break  # Nothing more to do, we bootstrapped with the latest schema version
        elif current == 'v4':
            db_ensure_schema_v5(cur)
            db_upgrade_data_v4_v5(cur)
            db_set_schema_version(cur, 'v4-v5')
            conn.commit()
        elif current == 'v4-v5':
            db_post_upgrade_v4_v5(cur)
            db_set_schema_version(cur, 'v5')
        elif current >= SCHEMA_VERSIONS[-1]:  # bail if version is too new (but hope it's compatible)
            break
        else:
            raise RuntimeError('unknown schema version')


def db_ensure_schema(conn: connection):
    with conn.cursor() as cur:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS meta (
            key text PRIMARY KEY,
            value text NOT NULL
        );
        ''')
        conn.commit()  # apparently, DDL is transactional with Postgres, so be sure to relieve the journal
        db_upgrade_schema(conn, cur)
    # the following could at some point be more adequate than the call to db_upgrade_schema above
    # -- namely, if the service is run as multiple processes and the upgrade must be done in advance --:
    # current, expected = db_get_schema_version(cur), SCHEMA_VERSIONS[-1]
    # if current != expected:
    #     raise SchemaVersionError(f"Database schema outdated! Expected {expected!r}, got {current!r}")


def db_update_account(cur: cursor, record: dict):
    sanitized = sanitize_record(record, ACCOUNT_DEFAULTS)
    cur.execute('''
    INSERT INTO account (subject, roles, "group")
    VALUES (%(subject)s, %(roles)s, %(group)s)
    ON CONFLICT (subject)
    DO UPDATE
    SET roles = EXCLUDED.roles,
    "group" = EXCLUDED."group"
    RETURNING accountid;''', sanitized)
    accountid, = cur.fetchone()
    return accountid


def db_filter_accounts(cur: cursor, predicate: callable):
    cur.execute('SELECT accountid FROM account;')
    removeids = [row[0] for row in cur.fetchall() if not predicate(*row)]
    while removeids:
        cur.execute('DELETE FROM account WHERE accountid IN %s', (tuple(removeids[:10]), ))
        del removeids[:10]


def db_clear_delegates(cur: cursor, accountid):
    cur.execute('''DELETE FROM delegation WHERE accountid = %s;''', (accountid, ))


def db_add_delegate(cur: cursor, accountid, delegate):
    cur.execute('''
    INSERT INTO delegation (accountid, delegateid)
    (SELECT %s, accountid
    FROM account
    WHERE subject = %s)
    RETURNING accountid;''', (accountid, delegate))


def db_find_subjects(cur: cursor, delegate):
    cur.execute('''
    SELECT a.subject
    FROM delegation
    JOIN account a ON a.accountid = delegation.accountid
    JOIN account b ON b.accountid = delegation.delegateid
    WHERE b.subject = %s;''', (delegate, ))
    return [row[0] for row in cur.fetchall()]


def db_get_group(cur: cursor, group):
    cur.execute('''SELECT subject FROM account WHERE "group" = %s;''', (group, ))
    return [row[0] for row in cur.fetchall()]


def db_get_groups(cur: cursor):
    cur.execute('''SELECT subject, "group" FROM account WHERE "group" is not null and "group" != '';''')
    groups = defaultdict(list)
    for row in cur.fetchall():
        groups[row[1]].append(row[0])
    return groups


def db_update_apikey(cur: cursor, accountid, apikey_hash):
    sanitized = dict(accountid=accountid, apikey_hash=apikey_hash)
    cur.execute('''
    INSERT INTO apikey (apikeyhash, accountid)
    VALUES (%(apikey_hash)s, %(accountid)s)
    ON CONFLICT (accountid, apikeyhash)
    DO UPDATE
    SET apikeyhash = EXCLUDED.apikeyhash  -- changes nothing, but necessary for RETURNING
    RETURNING apikeyid;''', sanitized)
    apikeyid, = cur.fetchone()
    return apikeyid


def db_filter_apikeys(cur: cursor, accountid, predicate: callable):
    cur.execute('SELECT apikeyid FROM apikey WHERE accountid = %s;', (accountid, ))
    removeids = [row[0] for row in cur.fetchall() if not predicate(*row)]
    while removeids:
        cur.execute('DELETE FROM apikey WHERE apikeyid IN %s', (tuple(removeids[:10]), ))
        del removeids[:10]


def db_update_publickey(cur: cursor, accountid, record: dict):
    sanitized = sanitize_record(record, PUBLIC_KEY_DEFAULTS, accountid=accountid)
    cur.execute('''
    INSERT INTO publickey (key, keytype, keyname, accountid)
    VALUES (%(public_key)s, %(public_key_type)s, %(public_key_name)s, %(accountid)s)
    ON CONFLICT (accountid, keyname)
    DO UPDATE
    SET key = EXCLUDED.key
    , keytype = EXCLUDED.keytype
    , keyname = EXCLUDED.keyname
    RETURNING keyid;''', sanitized)
    keyid, = cur.fetchone()
    return keyid


def db_filter_publickeys(cur: cursor, accountid, predicate: callable):
    cur.execute('SELECT keyid, keyname FROM publickey WHERE accountid = %s;', (accountid, ))
    removeids = [row[0] for row in cur.fetchall() if not predicate(*row)]
    while removeids:
        cur.execute('DELETE FROM publickey WHERE keyid IN %s', (tuple(removeids[:10]), ))
        del removeids[:10]


def db_get_report(cur: cursor, report_uuid):
    cur.execute(
        "SELECT data FROM report WHERE reportuuid = %(reportuuid)s;",
        {"reportuuid": report_uuid},
    )
    return [row[0] for row in cur.fetchall()]


def db_get_reports(cur: cursor, subject, limit, skip):
    cur.execute(
        sql.SQL("SELECT data FROM report {} LIMIT %(limit)s OFFSET %(skip)s;")
        .format(make_where_clause(
            None if not subject else sql.SQL('subject = %(subject)s'),
        )),
        {"subject": subject, "limit": limit, "skip": skip},
    )
    return [row[0] for row in cur.fetchall()]


def db_insert_report(cur: cursor, uuid, checked_at, subject, json_text):
    # this is an exception in that we don't use a record parameter (it's just not as practical here)
    cur.execute('''
    INSERT INTO report (reportuuid, checked_at, subject, data)
    VALUES (%s, %s, %s, %s)
    RETURNING reportid;''', (uuid, checked_at, subject, json_text))
    reportid, = cur.fetchone()
    return reportid


def db_insert_attestation(
    cur: cursor, guid, checked_at, subject, predicate, object_, reportid
):
    # this is an exception in that we don't use a record parameter (it's just not as practical here)
    cur.execute('''
    INSERT INTO attestation (guid, checked_at, subject, predicate, object, reportid)
    VALUES (%s, %s, %s, %s, %s, %s)
    RETURNING resultid;''', (guid, checked_at, subject, predicate, object_, reportid))
    resultid, = cur.fetchone()
    return resultid


def db_get_relevant_attestations(cur: cursor, subject=None):
    """for each combination of subject/object, get the most recent predicate that is still valid"""
    # DISTINCT ON is a Postgres-specific construct that comes in very handy here :)
    cur.execute(sql.SQL('''
    SELECT DISTINCT ON (subject, object)
    subject, object, predicate, guid, result2.checked_at, report.reportuuid
    FROM attestation
    JOIN report ON report.reportid = result2.reportid
    {filter_condition}
    ORDER BY subject, object, checked_at DESC;
    ''').format(
        filter_condition=make_where_clause(
            None if subject is None else sql.SQL('result2.subject = %(subject)s'),
        ),
    ), {"subject": subject})
    return cur.fetchall()

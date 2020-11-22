use super::{Result, Sql};
use crate::constants::ShowEmails;
use crate::context::Context;
use crate::paramsv;

pub async fn run(
    context: &Context,
    sql: &Sql,
    dbversion_before_update: i32,
    exists_before_update: bool,
) -> Result<(bool, bool)> {
    let mut dbversion = dbversion_before_update;
    let mut recalc_fingerprints = false;
    let mut update_icons = !exists_before_update;

    if dbversion < 1 {
        info!(context, "[migration] v1");
        sql.execute(
            "CREATE TABLE leftgrps ( id INTEGER PRIMARY KEY, grpid TEXT DEFAULT '');",
            paramsv![],
        )
        .await?;
        sql.execute(
            "CREATE INDEX leftgrps_index1 ON leftgrps (grpid);",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 1).await?;
    }
    if dbversion < 2 {
        info!(context, "[migration] v2");
        sql.execute(
            "ALTER TABLE contacts ADD COLUMN authname TEXT DEFAULT '';",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 2).await?;
    }
    if dbversion < 7 {
        info!(context, "[migration] v7");
        sql.execute(
            "CREATE TABLE keypairs (\
                 id INTEGER PRIMARY KEY, \
                 addr TEXT DEFAULT '' COLLATE NOCASE, \
                 is_default INTEGER DEFAULT 0, \
                 private_key, \
                 public_key, \
                 created INTEGER DEFAULT 0);",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 7).await?;
    }
    if dbversion < 10 {
        info!(context, "[migration] v10");
        sql.execute(
            "CREATE TABLE acpeerstates (\
                 id INTEGER PRIMARY KEY, \
                 addr TEXT DEFAULT '' COLLATE NOCASE, \
                 last_seen INTEGER DEFAULT 0, \
                 last_seen_autocrypt INTEGER DEFAULT 0, \
                 public_key, \
                 prefer_encrypted INTEGER DEFAULT 0);",
            paramsv![],
        )
        .await?;
        sql.execute(
            "CREATE INDEX acpeerstates_index1 ON acpeerstates (addr);",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 10).await?;
    }
    if dbversion < 12 {
        info!(context, "[migration] v12");
        sql.execute(
            "CREATE TABLE msgs_mdns ( msg_id INTEGER,  contact_id INTEGER);",
            paramsv![],
        )
        .await?;
        sql.execute(
            "CREATE INDEX msgs_mdns_index1 ON msgs_mdns (msg_id);",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 12).await?;
    }
    if dbversion < 17 {
        info!(context, "[migration] v17");
        sql.execute(
            "ALTER TABLE chats ADD COLUMN archived INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute("CREATE INDEX chats_index2 ON chats (archived);", paramsv![])
            .await?;
        // 'starred' column is not used currently
        // (dropping is not easily doable and stop adding it will make reusing it complicated)
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN starred INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute("CREATE INDEX msgs_index5 ON msgs (starred);", paramsv![])
            .await?;
        sql.set_raw_config_int(context, "dbversion", 17).await?;
    }
    if dbversion < 18 {
        info!(context, "[migration] v18");
        sql.execute(
            "ALTER TABLE acpeerstates ADD COLUMN gossip_timestamp INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE acpeerstates ADD COLUMN gossip_key;",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 18).await?;
    }
    if dbversion < 27 {
        info!(context, "[migration] v27");
        // chat.id=1 and chat.id=2 are the old deaddrops,
        // the current ones are defined by chats.blocked=2
        sql.execute("DELETE FROM msgs WHERE chat_id=1 OR chat_id=2;", paramsv![])
            .await?;
        sql.execute(
            "CREATE INDEX chats_contacts_index2 ON chats_contacts (contact_id);",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN timestamp_sent INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN timestamp_rcvd INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 27).await?;
    }
    if dbversion < 34 {
        info!(context, "[migration] v34");
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN hidden INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE msgs_mdns ADD COLUMN timestamp_sent INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE acpeerstates ADD COLUMN public_key_fingerprint TEXT DEFAULT '';",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE acpeerstates ADD COLUMN gossip_key_fingerprint TEXT DEFAULT '';",
            paramsv![],
        )
        .await?;
        sql.execute(
            "CREATE INDEX acpeerstates_index3 ON acpeerstates (public_key_fingerprint);",
            paramsv![],
        )
        .await?;
        sql.execute(
            "CREATE INDEX acpeerstates_index4 ON acpeerstates (gossip_key_fingerprint);",
            paramsv![],
        )
        .await?;
        recalc_fingerprints = true;
        sql.set_raw_config_int(context, "dbversion", 34).await?;
    }
    if dbversion < 39 {
        info!(context, "[migration] v39");
        sql.execute(
                "CREATE TABLE tokens ( id INTEGER PRIMARY KEY, namespc INTEGER DEFAULT 0, foreign_id INTEGER DEFAULT 0, token TEXT DEFAULT '', timestamp INTEGER DEFAULT 0);",
                paramsv![]
            ).await?;
        sql.execute(
            "ALTER TABLE acpeerstates ADD COLUMN verified_key;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE acpeerstates ADD COLUMN verified_key_fingerprint TEXT DEFAULT '';",
            paramsv![],
        )
        .await?;
        sql.execute(
            "CREATE INDEX acpeerstates_index5 ON acpeerstates (verified_key_fingerprint);",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 39).await?;
    }
    if dbversion < 40 {
        info!(context, "[migration] v40");
        sql.execute(
            "ALTER TABLE jobs ADD COLUMN thread INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 40).await?;
    }
    if dbversion < 44 {
        info!(context, "[migration] v44");
        sql.execute("ALTER TABLE msgs ADD COLUMN mime_headers TEXT;", paramsv![])
            .await?;
        sql.set_raw_config_int(context, "dbversion", 44).await?;
    }
    if dbversion < 46 {
        info!(context, "[migration] v46");
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN mime_in_reply_to TEXT;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN mime_references TEXT;",
            paramsv![],
        )
        .await?;
        dbversion = 46;
        sql.set_raw_config_int(context, "dbversion", 46).await?;
    }
    if dbversion < 47 {
        info!(context, "[migration] v47");
        sql.execute(
            "ALTER TABLE jobs ADD COLUMN tries INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 47).await?;
    }
    if dbversion < 48 {
        info!(context, "[migration] v48");
        // NOTE: move_state is not used anymore
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN move_state INTEGER DEFAULT 1;",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 48).await?;
    }
    if dbversion < 49 {
        info!(context, "[migration] v49");
        sql.execute(
            "ALTER TABLE chats ADD COLUMN gossiped_timestamp INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 49).await?;
    }
    if dbversion < 50 {
        info!(context, "[migration] v50");
        // installations <= 0.100.1 used DC_SHOW_EMAILS_ALL implicitly;
        // keep this default and use DC_SHOW_EMAILS_NO
        // only for new installations
        if exists_before_update {
            sql.set_raw_config_int(context, "show_emails", ShowEmails::All as i32)
                .await?;
        }
        sql.set_raw_config_int(context, "dbversion", 50).await?;
    }
    if dbversion < 53 {
        info!(context, "[migration] v53");
        // the messages containing _only_ locations
        // are also added to the database as _hidden_.
        sql.execute(
                "CREATE TABLE locations ( id INTEGER PRIMARY KEY AUTOINCREMENT, latitude REAL DEFAULT 0.0, longitude REAL DEFAULT 0.0, accuracy REAL DEFAULT 0.0, timestamp INTEGER DEFAULT 0, chat_id INTEGER DEFAULT 0, from_id INTEGER DEFAULT 0);",
                paramsv![]
            ).await?;
        sql.execute(
            "CREATE INDEX locations_index1 ON locations (from_id);",
            paramsv![],
        )
        .await?;
        sql.execute(
            "CREATE INDEX locations_index2 ON locations (timestamp);",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE chats ADD COLUMN locations_send_begin INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE chats ADD COLUMN locations_send_until INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE chats ADD COLUMN locations_last_sent INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "CREATE INDEX chats_index3 ON chats (locations_send_until);",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 53).await?;
    }
    if dbversion < 54 {
        info!(context, "[migration] v54");
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN location_id INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "CREATE INDEX msgs_index6 ON msgs (location_id);",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 54).await?;
    }
    if dbversion < 55 {
        info!(context, "[migration] v55");
        sql.execute(
            "ALTER TABLE locations ADD COLUMN independent INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 55).await?;
    }
    if dbversion < 59 {
        info!(context, "[migration] v59");
        // records in the devmsglabels are kept when the message is deleted.
        // so, msg_id may or may not exist.
        sql.execute(
                "CREATE TABLE devmsglabels (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT, msg_id INTEGER DEFAULT 0);",
                paramsv![],
            ).await?;
        sql.execute(
            "CREATE INDEX devmsglabels_index1 ON devmsglabels (label);",
            paramsv![],
        )
        .await?;
        if exists_before_update && sql.get_raw_config_int(context, "bcc_self").await.is_none() {
            sql.set_raw_config_int(context, "bcc_self", 1).await?;
        }
        sql.set_raw_config_int(context, "dbversion", 59).await?;
    }
    if dbversion < 60 {
        info!(context, "[migration] v60");
        sql.execute(
            "ALTER TABLE chats ADD COLUMN created_timestamp INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 60).await?;
    }
    if dbversion < 61 {
        info!(context, "[migration] v61");
        sql.execute(
            "ALTER TABLE contacts ADD COLUMN selfavatar_sent INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        update_icons = true;
        sql.set_raw_config_int(context, "dbversion", 61).await?;
    }
    if dbversion < 62 {
        info!(context, "[migration] v62");
        sql.execute(
            "ALTER TABLE chats ADD COLUMN muted_until INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 62).await?;
    }
    if dbversion < 63 {
        info!(context, "[migration] v63");
        sql.execute("UPDATE chats SET grpid='' WHERE type=100", paramsv![])
            .await?;
        sql.set_raw_config_int(context, "dbversion", 63).await?;
    }
    if dbversion < 64 {
        info!(context, "[migration] v64");
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN error TEXT DEFAULT '';",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 64).await?;
    }
    if dbversion < 65 {
        info!(context, "[migration] v65");
        sql.execute(
            "ALTER TABLE chats ADD COLUMN ephemeral_timer INTEGER",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN ephemeral_timer INTEGER DEFAULT 0",
            paramsv![],
        )
        .await?;
        sql.execute(
            "ALTER TABLE msgs ADD COLUMN ephemeral_timestamp INTEGER DEFAULT 0",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 65).await?;
    }
    if dbversion < 66 {
        info!(context, "[migration] v66");
        update_icons = true;
        sql.set_raw_config_int(context, "dbversion", 66).await?;
    }
    if dbversion < 67 {
        info!(context, "[migration] v67");
        for prefix in &["", "configured_"] {
            if let Some(server_flags) = sql
                .get_raw_config_int(context, format!("{}server_flags", prefix))
                .await
            {
                let imap_socket_flags = server_flags & 0x700;
                let key = format!("{}mail_security", prefix);
                match imap_socket_flags {
                    0x100 => sql.set_raw_config_int(context, key, 2).await?, // STARTTLS
                    0x200 => sql.set_raw_config_int(context, key, 1).await?, // SSL/TLS
                    0x400 => sql.set_raw_config_int(context, key, 3).await?, // Plain
                    _ => sql.set_raw_config_int(context, key, 0).await?,
                }
                let smtp_socket_flags = server_flags & 0x70000;
                let key = format!("{}send_security", prefix);
                match smtp_socket_flags {
                    0x10000 => sql.set_raw_config_int(context, key, 2).await?, // STARTTLS
                    0x20000 => sql.set_raw_config_int(context, key, 1).await?, // SSL/TLS
                    0x40000 => sql.set_raw_config_int(context, key, 3).await?, // Plain
                    _ => sql.set_raw_config_int(context, key, 0).await?,
                }
            }
        }
        sql.set_raw_config_int(context, "dbversion", 67).await?;
    }
    if dbversion < 68 {
        info!(context, "[migration] v68");
        // the index is used to speed up get_fresh_msg_cnt() (see comment there for more details) and marknoticed_chat()
        sql.execute(
            "CREATE INDEX IF NOT EXISTS msgs_index7 ON msgs (state, hidden, chat_id);",
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 68).await?;
    }
    if dbversion < 69 {
        info!(context, "[migration] v69");
        sql.execute(
            "ALTER TABLE chats ADD COLUMN protected INTEGER DEFAULT 0;",
            paramsv![],
        )
        .await?;
        sql.execute(
            "UPDATE chats SET protected=1, type=120 WHERE type=130;", // 120=group, 130=old verified group
            paramsv![],
        )
        .await?;
        sql.set_raw_config_int(context, "dbversion", 69).await?;
    }

    Ok((recalc_fingerprints, update_icons))
}

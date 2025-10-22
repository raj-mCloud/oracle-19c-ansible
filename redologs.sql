SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF VERIFY OFF PAGES 0 LINES 300 TRIMSPOOL ON

DECLARE
  c_dg1 CONSTANT VARCHAR2(128) := '+REDODG1';
  c_dg2 CONSTANT VARCHAR2(128) := '+REDODG2';
  c_dg3 CONSTANT VARCHAR2(128) := '+REDODG3';

  c_max_attempts PLS_INTEGER := 40;
  c_sleep_secs   PLS_INTEGER := 5;

  v_added        PLS_INTEGER := 0;
  v_dropped      PLS_INTEGER := 0;
  v_skipped      PLS_INTEGER := 0;

  CURSOR c_logs IS
    SELECT l.group#, l.thread#, l.status, l.bytes
    FROM   v$log l
    ORDER  BY l.group#;

  PROCEDURE get_pair(p_group NUMBER, p_out1 OUT VARCHAR2, p_out2 OUT VARCHAR2) IS
    v_key INTEGER := CASE WHEN MOD(p_group,3)=0 THEN 3 ELSE MOD(p_group,3) END;
  BEGIN
    IF v_key = 1 THEN p_out1 := c_dg1; p_out2 := c_dg2;       -- (1,2)
    ELSIF v_key = 2 THEN p_out1 := c_dg2; p_out2 := c_dg3;    -- (2,3)
    ELSE                  p_out1 := c_dg1; p_out2 := c_dg3;   -- (1,3)
    END IF;
  END;

  FUNCTION has_member_in_dg(p_group NUMBER, p_dg VARCHAR2) RETURN BOOLEAN IS
    v_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM   v$logfile
    WHERE  group# = p_group
    AND    member LIKE p_dg || '/%';
    RETURN v_cnt > 0;
  END;

  FUNCTION count_valid_targets(p_group NUMBER, p_t1 VARCHAR2, p_t2 VARCHAR2) RETURN PLS_INTEGER IS
    v_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO v_cnt
      FROM v$logfile
     WHERE group# = p_group
       AND status IS NULL
       AND (member LIKE p_t1 || '/%' OR member LIKE p_t2 || '/%');
    RETURN v_cnt;
  END;

BEGIN
  DBMS_OUTPUT.PUT_LINE('--- Redo multiplexing with safe validation of new members ---');

  FOR r IN c_logs LOOP
    DECLARE
      v_t1 VARCHAR2(128);
      v_t2 VARCHAR2(128);
      v_status VARCHAR2(20);
      v_attempts PLS_INTEGER := 0;
      v_total_members PLS_INTEGER;
      v_non_target_count PLS_INTEGER;
      v_valid_targets PLS_INTEGER;
    BEGIN
      get_pair(r.group#, v_t1, v_t2);
      DBMS_OUTPUT.PUT_LINE('Group '||r.group#||' thread='||r.thread#||
                           ' status='||r.status||' sizeMB='||(r.bytes/1024/1024));
      DBMS_OUTPUT.PUT_LINE('  Target pair: '||v_t1||', '||v_t2);

      IF NOT has_member_in_dg(r.group#, v_t1) THEN
        EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE MEMBER '''||v_t1||''' TO GROUP '||r.group#;
        DBMS_OUTPUT.PUT_LINE('  Added member in '||v_t1);
        v_added := v_added + 1;
      END IF;

      IF NOT has_member_in_dg(r.group#, v_t2) THEN
        EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE MEMBER '''||v_t2||''' TO GROUP '||r.group#;
        DBMS_OUTPUT.PUT_LINE('  Added member in '||v_t2);
        v_added := v_added + 1;
      END IF;

      v_attempts := 0;
      v_valid_targets := count_valid_targets(r.group#, v_t1, v_t2);

      WHILE v_valid_targets < 2 AND v_attempts < c_max_attempts LOOP
        DBMS_OUTPUT.PUT_LINE('  Need VALID copies on targets (have '||v_valid_targets||'); SWITCH LOGFILE (attempt '||(v_attempts+1)||'/'||c_max_attempts||')');
        EXECUTE IMMEDIATE 'ALTER SYSTEM SWITCH LOGFILE';
        DBMS_LOCK.SLEEP(c_sleep_secs);
        v_attempts := v_attempts + 1;
        v_valid_targets := count_valid_targets(r.group#, v_t1, v_t2);
      END LOOP;

      IF v_valid_targets < 2 THEN
        DBMS_OUTPUT.PUT_LINE('  WARN: Could not get two VALID target members yet; will not drop old members now.');
        v_skipped := v_skipped + 1;
        CONTINUE;
      END IF;

      SELECT status INTO v_status FROM v$log WHERE group# = r.group# AND thread# = r.thread#;

      v_attempts := 0;
      WHILE v_status NOT IN ('INACTIVE','UNUSED') AND v_attempts < c_max_attempts LOOP
        DBMS_OUTPUT.PUT_LINE('  Group '||r.group#||' is '||v_status||'; SWITCH LOGFILE (attempt '||(v_attempts+1)||'/'||c_max_attempts||')');
        EXECUTE IMMEDIATE 'ALTER SYSTEM SWITCH LOGFILE';
        DBMS_LOCK.SLEEP(c_sleep_secs);
        v_attempts := v_attempts + 1;
        SELECT status INTO v_status FROM v$log WHERE group# = r.group# AND thread# = r.thread#;
      END LOOP;

      IF v_status NOT IN ('INACTIVE','UNUSED') THEN
        DBMS_OUTPUT.PUT_LINE('  WARN: Group stayed '||v_status||'; skip drop this run.');
        v_skipped := v_skipped + 1;
        CONTINUE;
      END IF;

      SELECT COUNT(*) INTO v_non_target_count
      FROM   v$logfile
      WHERE  group# = r.group#
      AND    NOT (member LIKE v_t1 || '/%' OR member LIKE v_t2 || '/%');

      IF v_non_target_count > 0 THEN
        FOR m IN (
          SELECT member
          FROM   v$logfile
          WHERE  group# = r.group#
          AND    NOT (member LIKE v_t1 || '/%' OR member LIKE v_t2 || '/%')
          ORDER  BY member
        ) LOOP
          SELECT COUNT(*) INTO v_total_members FROM v$logfile WHERE group# = r.group#;

          IF v_total_members > 2 THEN
            EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE MEMBER '''||REPLACE(m.member, '''', '''''')||'''';
            DBMS_OUTPUT.PUT_LINE('  Dropped non-target: '||m.member);
            v_dropped := v_dropped + 1;
          ELSE
            DBMS_OUTPUT.PUT_LINE('  Keep: need ≥2 members; not dropping '||m.member);
          END IF;
        END LOOP;
      END IF;

    END;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('--- Summary ---');
  DBMS_OUTPUT.PUT_LINE('  Members added   : '||v_added);
  DBMS_OUTPUT.PUT_LINE('  Members dropped : '||v_dropped);
  DBMS_OUTPUT.PUT_LINE('  Groups deferred : '||v_skipped||' (not yet VALID/INACTIVE)');
  DBMS_OUTPUT.PUT_LINE('STATUS: Completed.');
END;
/
EXIT

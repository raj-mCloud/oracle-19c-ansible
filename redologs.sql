-- multiplex_redo_compact.sql
SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF VERIFY OFF PAGES 0 LINES 300

DECLARE
  -- target ASM diskgroups
  g1 CONSTANT VARCHAR2(64) := '+REDODG1';
  g2 CONSTANT VARCHAR2(64) := '+REDODG2';
  g3 CONSTANT VARCHAR2(64) := '+REDODG3';

  -- polling
  max_try PLS_INTEGER := 120;   -- attempts for each wait
  nap     PLS_INTEGER := 5;     -- seconds between attempts

  -- map group -> (t1,t2) as (1,2),(2,3),(1,3)
  PROCEDURE pair_for(p_grp NUMBER, t1 OUT VARCHAR2, t2 OUT VARCHAR2) IS
    k PLS_INTEGER := CASE WHEN MOD(p_grp,3)=0 THEN 3 ELSE MOD(p_grp,3) END;
  BEGIN
    IF k=1 THEN t1:=g1; t2:=g2;
    ELSIF k=2 THEN t1:=g2; t2:=g3;
    ELSE           t1:=g1; t2:=g3;
    END IF;
  END;

  FUNCTION has_member(p_grp NUMBER, p_dg VARCHAR2) RETURN BOOLEAN IS v NUMBER;
  BEGIN
    SELECT COUNT(*) INTO v FROM v$logfile
     WHERE group#=p_grp AND member LIKE p_dg||'/%';
    RETURN v>0;
  END;

  FUNCTION valid_targets(p_grp NUMBER, t1 VARCHAR2, t2 VARCHAR2) RETURN NUMBER IS v NUMBER;
  BEGIN
    -- VALID = status IS NULL
    SELECT COUNT(*) INTO v FROM v$logfile
     WHERE group#=p_grp AND status IS NULL
       AND (member LIKE t1||'/%' OR member LIKE t2||'/%');
    RETURN v;
  END;

  PROCEDURE sw IS BEGIN EXECUTE IMMEDIATE 'ALTER SYSTEM SWITCH LOGFILE'; END;

BEGIN
  DBMS_OUTPUT.PUT_LINE('--- redo multiplexing (compact) ---');

  FOR r IN (SELECT group#, thread#, bytes FROM v$log ORDER BY group#) LOOP
    DECLARE
      t1 VARCHAR2(64); t2 VARCHAR2(64);
      tries PLS_INTEGER; v NUMBER; s VARCHAR2(20);
      tot NUMBER; dropped BOOLEAN;
    BEGIN
      pair_for(r.group#, t1, t2);
      DBMS_OUTPUT.PUT_LINE('Group '||r.group#||' targets: '||t1||','||t2);

      -- add missing target members
      IF NOT has_member(r.group#, t1) THEN
        EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE MEMBER '''||t1||''' TO GROUP '||r.group#;
      END IF;
      IF NOT has_member(r.group#, t2) THEN
        EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE MEMBER '''||t2||''' TO GROUP '||r.group#;
      END IF;

      -- wait: both target members VALID
      tries := 0; v := valid_targets(r.group#, t1, t2);
      WHILE v<2 AND tries<max_try LOOP
        sw; DBMS_LOCK.SLEEP(nap); tries := tries+1; v := valid_targets(r.group#, t1, t2);
      END LOOP;
      IF v<2 THEN RAISE_APPLICATION_ERROR(-20001,'Timeout: 2 VALID members not reached for group '||r.group#); END IF;

      -- wait: group INACTIVE/UNUSED
      tries := 0; SELECT status INTO s FROM v$log WHERE group#=r.group# AND thread#=r.thread#;
      WHILE s NOT IN ('INACTIVE','UNUSED') AND tries<max_try LOOP
        sw; DBMS_LOCK.SLEEP(nap); tries := tries+1;
        SELECT status INTO s FROM v$log WHERE group#=r.group# AND thread#=r.thread#;
      END LOOP;
      IF s NOT IN ('INACTIVE','UNUSED') THEN RAISE_APPLICATION_ERROR(-20002,'Timeout: group '||r.group#||' not INACTIVE'); END IF;

      -- drop non-targets while keeping >=2
      LOOP
        dropped := FALSE;
        SELECT COUNT(*) INTO tot FROM v$logfile WHERE group#=r.group#;
        EXIT WHEN tot<=2;

        FOR x IN (
          SELECT member FROM v$logfile
           WHERE group#=r.group# AND NOT (member LIKE t1||'/%' OR member LIKE t2||'/%')
           ORDER BY member
        ) LOOP
          SELECT COUNT(*) INTO tot FROM v$logfile WHERE group#=r.group#;
          EXIT WHEN tot<=2;
          EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE MEMBER '''||REPLACE(x.member,'''','''''')||'''';
          dropped := TRUE;
        END LOOP;

        EXIT WHEN dropped=FALSE; -- no non-targets left
      END LOOP;

      -- if still >2, trim to exactly 2 while keeping one in each target DG
      SELECT COUNT(*) INTO tot FROM v$logfile WHERE group#=r.group#;
      IF tot>2 THEN
        DECLARE keep1 BOOLEAN:=FALSE; keep2 BOOLEAN:=FALSE;
        BEGIN
          FOR y IN (SELECT member FROM v$logfile WHERE group#=r.group# ORDER BY member) LOOP
            SELECT COUNT(*) INTO tot FROM v$logfile WHERE group#=r.group#;
            EXIT WHEN tot<=2;
            IF    y.member LIKE t1||'/%' AND NOT keep1 THEN keep1:=TRUE; 
            ELSIF y.member LIKE t2||'/%' AND NOT keep2 THEN keep2:=TRUE;
            ELSE
              EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE MEMBER '''||REPLACE(y.member,'''','''''')||'''';
            END IF;
          END LOOP;
        END;
      END IF;

      DBMS_OUTPUT.PUT_LINE('  -> Group '||r.group#||' aligned to exactly two members on target DGs.');
    END;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('STATUS: done.');
END;
/
EXIT

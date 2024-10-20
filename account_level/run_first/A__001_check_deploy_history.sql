-- ---------------------------------------------------------------------------------------------------------------------------
-- Check to see if the R script has been run for the particular enironment
-- If script is not in deploy history with the environment name suffix, 
-- then rename the script file value from previous run so that the script can execute again with the default name.
-- We need to have this script in a different folder from the other account level scripts to control execution order;
-- schema_change runs 'A' scripts after all others.
-- ---------------------------------------------------------------------------------------------------------------------------
UPDATE util_db.schemachange.change_history ch
SET ch.script = ch.script || ' : ' || dh.env_variable
FROM util_db.public.deploy_history dh
WHERE ch.script LIKE 'R__00%.sql' AND ch.status = 'Success'
AND ch.script != ch.script || ' : ' || dh.env_variable
AND ch.installed_on = dh.installed_on;

-- set script name back to standard for an older run so that it won't run again;
-- it will get updated back with environment name if/when the next new environment gets deployed.
UPDATE util_db.schemachange.change_history ch
SET ch.script = TRIM(split_part(ch.script,':',1))
FROM util_db.public.deploy_history dh
WHERE ch.status = 'Success'
AND ch.script like 'R__00%.sql% : ' || '{{ environment }}'
AND ch.installed_on = dh.installed_on;

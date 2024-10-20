-- ******************************************************************************************************************************
-- Create procedure to test all views
-- if devops fails at this step, check snowflake query history to see which view was the last failure   
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE utils.test_all_views_sp()
  RETURNS string
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS
$$
    var returnValue = "";
    var listViews = `SELECT
					table_catalog AS database_name,
					table_schema AS schema_name,
					table_name AS view_name,
					last_altered
					FROM information_schema.views
					WHERE 
						table_schema NOT IN ('INFORMATION_SCHEMA')
						AND NOT (table_name ilike any ('%time%series%', '%connectivity_%_detail%' ,'pipes_status','%total_elec_energy_sensor%','%hvac_occupancy_15minute%','%hvac_sensor_reading%','%occupancy%15%minute%'))
					ORDER BY table_schema, table_name;`;
    var stmt = snowflake.createStatement( {sqlText: listViews} );
    var rowsResult = stmt.execute(); 
    
    while(rowsResult.next()) {
        returnValue = "";
        returnValue += rowsResult.getColumnValue(1);
        returnValue += "." + rowsResult.getColumnValue(2);
		returnValue += "." + rowsResult.getColumnValue(3);
        var sqlCommand = "SELECT TOP 1 * FROM " + returnValue;
        var stmts = snowflake.createStatement({ sqlText: sqlCommand });
        var sqlResult = stmts.execute()
    }
    
    return 'OK';
$$;
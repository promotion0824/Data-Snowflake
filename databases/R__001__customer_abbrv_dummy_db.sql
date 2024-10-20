-- ------------------------------------------------------------------------------------------------------------------------------
-- Create  _<customerAbbrv> database
-- This database is only used to help to identify customer account
-- ------------------------------------------------------------------------------------------------------------------------------

USE ROLE {{ defaultRole }};

CREATE DATABASE IF NOT EXISTS _{{ customerAbbrv }};
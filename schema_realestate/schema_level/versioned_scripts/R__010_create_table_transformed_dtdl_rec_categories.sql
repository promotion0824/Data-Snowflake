----------------------------------------------------------------------------------
-- Create table for storing aggregates at hour level
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE transformed.dtdl_rec_categories (
	category_name	      	VARCHAR(36),
	dtdl_rec_category      	VARCHAR(36),
    description           	VARCHAR(2000),
	_created_at           	TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE(),
	_last_updated_at      	TIMESTAMP_NTZ       NOT NULL    DEFAULT SYSDATE()
);

INSERT INTO transformed.dtdl_rec_categories (category_name,dtdl_rec_category,description)
VALUES
('Asset','Asset','An object which is placed inside of a building, but is not an integral part of the building structure, for example architectural, furniture, equipment, systems, etc.'),
('Agent','Stakeholder','Any basic types of stakeholder that can have roles or perform activities, e.g., people, companies, departments.'),
('Capability','Live Data','A capability indicates the capacity of a entity, be it a Space, an Asset, or a LogicalDevice, to produce or ingest data. This is equivalent to the term "point" in Brick Schema and generic Building Management System. Specific subclasses specialize this behavior: Sensor entities harvest data from the real world, Actuator entities accept commands from a digital twin platform, and Parameter entities configure some capability or system. For more detailed information, refer to the Capability Readme.'),
('Collection','Collection','An administrative grouping of entities that are addressed and treated as a unit for some purpose. These entities may have some spatial arrangement (e.g., an Apartment is typically contiguous),, however that is not a requirement (see, e.g., a distributed Campus consisting of spatially disjoint plots or buildings),.'),
('Document','Document','A formal piece of written, printed or electronic matter that provides information or evidence or that serves as an official record, for example LeaseContract, Building Specification, Warranty, Drawing, etc.'),
('Space','Space','A contiguous part of the physical world that has a 3D spatial extent and that contains or can contain sub-spaces. For example a Region can contain many pieces of Land, which in turn can contain many Buildings, which in turn can contain Levels and Rooms'),
('Building Component','Structural','A part that constitutes a piece of a building''s structural makeup, for example Facade, Wall, Slab, RoofInner, etc.')
;
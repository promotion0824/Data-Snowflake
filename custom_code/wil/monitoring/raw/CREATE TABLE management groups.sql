CREATE OR REPLACE TABLE central_monitoring_db.transformed.business_units (
	management_group VARCHAR(100),
	business_unit VARCHAR(100)
);

CREATE OR REPLACE TABLE central_monitoring_db.transformed.management_groups (
	management_group VARCHAR(100),
	subscription_name VARCHAR(100),
	subscription_id VARCHAR(36)
);

INSERT INTO central_monitoring_db.transformed.management_groups (subscription_id, subscription_name, management_group)
VALUES
('2bc9e8d9-c3c9-466d-af57-b61a2332aecc','Access to Azure Active Directory','CoreIT'),
('12773329-7c03-4316-aefe-eb79456685a0','Azure subscription 1','CoreIT'),
('cb2af084-edcb-43b1-8226-e15fdcfe24cb','Azure subscription 1','CoreIT'),
('ac560f4a-382e-4d77-98d8-6b4b4e2d2d7c','Cyber Security','CoreIT'),
('86d9ce64-bb7f-4c13-8c6f-8171e1d3b3eb','Microsoft Azure Enterprise','CoreIT'),
('8a5cfc7a-4218-44e5-9394-6b60f8596454','Sandbox-IT','CoreIT'),
('8aa30e28-0117-4d5c-b743-961f69a87d42','Visual Studio Enterprise Subscription - MPN','CoreIT'),
('e80f9326-901c-413e-9dfb-a37065fddb89','Visual Studio Enterprise Subscription - MPN','CoreIT'),
('b3d7d551-c796-425d-a8e8-1d58e77f1b3f','Visual Studio Enterprise Subscription - MPN','CoreIT'),
('d600f8c4-9c28-40ba-83a3-06df7350e673','Visual Studio Enterprise Subscription - MPN','CoreIT'),
('cbf567cd-b6be-43a2-9cc6-0aa58d333423','Visual Studio Enterprise Subscription - MPN','CoreIT'),
('0b761f31-419d-4710-b945-30c0eaf8268b','Visual Studio Enterprise Subscription - MPN','CoreIT'),
('6d80c285-a593-4116-bc75-1e54432af0d5','Visual Studio Enterprise Subscription - MPN','CoreIT'),
('ad6a7104-1f13-4b0b-ae9c-111a33730719','Visual Studio Enterprise Subscription - MPN','CoreIT'),
('79baa8ed-70da-4f3b-94a7-9e9d41eec061','Visual Studio Enterprise Subscription - MPN','CoreIT'),
('f628afbb-1d6b-46ad-85ca-366d4ea40264','Visual Studio Licences','CoreIT'),
('936e6b7e-33c4-44ac-826c-4364a389b74b','Willow-CoreIT','CoreIT'),
('a3a9921b-5f1b-4c4b-a539-4a335fbcc2fe','Willow-IT','CoreIT'),
('89a48b77-62f9-4bc3-8ddc-cd6db25fb0bc','Build-DEV','Product'),
('bdd7479b-ce38-47cf-b22f-285337c1e951','Build-PRD','Product'),
('ca5069fb-5f00-4df3-8491-10ee2768126a','Build-UAT','Product'),
('f89766c3-f955-42b5-8925-929bd7df83c0','Data-DEV','Product'),
('16c3dd19-5016-4bf8-85e2-8c7e88607b7a','Data-PRD','Product'),
('2e254e02-1149-4bb8-8db1-f20511afb1a4','Experience-DEV','Product'),
('3ab44a28-5c8d-4f57-9d5e-b4830331e5db','Experience-PRD','Product'),
('c9a0ae53-c468-4612-a318-cfb719c4b2ca','Experience-UAT','Product'),
('178b67d7-b6fd-46db-b4a3-b57f8a6b045f','K8S-INTERNAL','Product'),
('7dab7b5a-d968-43a5-a50f-9509244c297b','K8S-Internal-Environments','Product'),
('0be01d84-8432-4558-9aba-ecd204a3ee61','Platform-DEV','Product'),
('e878a98a-20ec-4516-a59d-f393429fe07c','Platform-PRD','Product'),
('f4edc7ff-4396-47be-bf14-90892e463848','Platform-UAT','Product'),
('d4746c7a-19cb-47ac-82b5-069b17cb99de','Products-Shared','Product'),
('5f077d49-cd08-48b4-a26b-59d708d7847b','Products-Shared-Environments','Product'),
('ae0c8612-75e8-4b6e-9443-ce2d7cd35d4e','Rail-DEMO','Product'),
('5b797013-f6dc-4235-a157-e4cb98ef8599','Rail-DEV','Product'),
('ee06cff9-d2d7-405a-8ce9-cf82fe78fbf6','Rail-PRD','Product'),
('1bc38ca0-6f7f-4f68-946f-5359d9ace66a','Rail-PRE','Product'),
('0c69c2fd-b86e-4f3a-9cb6-a72eb9de9902','Rail-UAT','Product'),
('249312a0-4c83-4d73-b164-18c5e72bf219','SandboxShared','Product'),
('48a16780-c719-4528-a0f2-4e7640a9c850','dev','Willow Twin Development'),
('3d662540-4bed-4624-8c4a-fde386ae6667','dev-eus-00','Willow Twin Development'),
('34278886-d081-45e9-b627-37859eed000e','dev-eus-01','Willow Twin Development'),
('f7036919-3d51-4ed4-9851-4df254e67023','dev-wus-01','Willow Twin Development'),
('fd259995-1de7-4ae8-8431-0d150dcca6f4','prd','Willow Twin Production'),
('30778047-59e1-4183-aa92-f0c21e221215','prd-aue-01','Willow Twin Production'),
('fec406ed-b645-4085-9432-38a2d228d484','prd-aue-02','Willow Twin Production'),
('60fb2721-bc6e-4fba-98b9-1023bc1d7f11','prd-aue-03','Willow Twin Production'),
('0bece55b-fdbc-4166-bf0f-1e8acf2ce8c3','prd-eus-01','Willow Twin Production'),
('aa62fdad-c619-4642-9920-e5bf2214cdd8','prd-eus-02','Willow Twin Production'),
('3fb669b2-4001-493d-9c4b-239fd840ecb9','prd-eus-03','Willow Twin Production'),
('192ffab7-c64e-4f35-9e1d-7d2ed0fbf6f1','prd-eus-04','Willow Twin Production'),
('d45624d2-b973-4d02-bc6f-99721e8cc311','prd-eus-05','Willow Twin Production'),
('52d36e42-7a4f-4798-ba4b-d828ee99ba69','prd-eus-06','Willow Twin Production'),
('8abe8cc8-96c9-4c30-8953-8dd6ef6cdc14','prd-eus-07','Willow Twin Production'),
('de45c72a-3ba0-4b25-90d1-647449084e12','prd-eus-08','Willow Twin Production'),
('6b86e92f-3156-4411-8902-284cd7936dbd','prd-eus-09','Willow Twin Production'),
('376d7e17-7ee5-4fa7-82fd-da4f8b59b139','prd-eus-10','Willow Twin Production'),
('4ad0e724-9964-46d0-a7ef-cf9ef9bfc628','prd-eus-11','Willow Twin Production'),
('a0f47fc5-9468-4995-8c4c-749bab2ea244','prd-eus-12','Willow Twin Production'),
('553f62fa-8dd2-4937-aa70-457fd3537578','prd-weu-01','Willow Twin Production'),
('CoreIT','CoreIT',''),
('Product','Product',''),
('d43166d1-c2a1-4f26-a213-f620dba13ab8','Tenant Root Group',''),
('WillowTwinDev','Willow Twin Development',''),
('WillowTwinPrd','Willow Twin Production','')
;

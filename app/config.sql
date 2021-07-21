create schema [ata]

drop table if exists ata.sites 
create table ata.sites (
	site_id int identity(1,1) primary key,
	ata_code int not null unique,
	ata_label varchar(255) not null
)

drop table if exists ata.columns
create table ata.columns (
	column_id int identity(1,1) primary key,
	ata_code varchar(255) not null unique,
	ata_label varchar(255) not null,
	notes varchar(4000) null
)

drop table if exists ata.requests
create table ata.requests (
	request_id int identity(1,1) primary key,
	import_schema varchar(20) not null,
	import_table varchar(100) not null,
	--site_id int not null foreign key references ata.sites(site_id),
	run_daily bit not null default(0),
	run_monthly bit not null default(1),
	run_yearly bit not null default(1),
	is_active bit not null default(1) 
)

drop table if exists ata.request_columns
create table ata.request_columns (
	request_id int not null foreign key references ata.requests(request_id),
	column_id int not null foreign key references ata.columns(column_id),
	primary key (request_id,column_id)
)

drop table if exists ata.request_sites
create table ata.request_sites (
	request_id int not null foreign key references ata.requests(request_id),
	site_id int not null foreign key references ata.sites(site_id),
	primary key (request_id,site_id)
)

-- some common metris/dimensions (this is not all of them)
insert into ata.columns (ata_code,ata_label,notes) values
('geo_country','Countries',null),
('geo_region','Regions','Province/State'),
('geo_metro','Sub-Regions','City'),
('m_visits','Visits','GA: Sessions'),
('m_page_loads','Page views',null),
('m_unique_visitors','Visitors','GA: Users'),
('site_id','Site ID','GA: View ID'),
('date_day','Weekday','Day of Week'),
('event_hour','Hour','Hour of action, standard time'),
('device_hour','Hour','Hour based on user device'),
('src','Source',null),
('page','Page','This include multiple parts, needs to be split'),
('src_detail','Source - Detail',null),
('device_type','Device',null),
('device_brand','Device - Brand','e.g. Apple'),
('device_name','Device - Marketing Name','e.g. iPhone'),
('date','Date',null),
('m_time_spent_per_pages_loads','Time spent (page) / page','Average time spent on page'),
('m_time_spent_loads','Time spent (pages)','Time spent on page'),
('m_bounces','Bounces',null)

-- these should be the same sites associated with your account
insert into ata.sites (ata_code,ata_label) values
(123456,'My First Site')

/*
delete from ata.request_sites
delete from ata.request_columns
delete from ata.requests
dbcc checkident('ata.requests',RESEED,0)
*/

-- sample requests
insert into ata.requests (import_schema,import_table) values ('imports','ata_location_by_page')
insert into ata.request_sites (request_id,site_id) values (1,4),(1,5),(1,6)
insert into ata.request_columns (request_id,column_id) values (1,1),(1,2),(1,12),(1,4),(1,5),(1,6),(1,7)

insert into ata.requests (import_schema,import_table) values ('imports','ata_time_of_day')
insert into ata.request_sites (request_id,site_id) values (2,4),(2,5),(2,6)
insert into ata.request_columns (request_id,column_id) values (2,8),(2,10),(2,7),(2,6)

insert into ata.requests (import_schema,import_table) values ('imports','ata_source_by_page')
insert into ata.request_sites (request_id,site_id) values (3,4),(3,5),(3,6)
insert into ata.request_columns (request_id,column_id) values (3,11),(3,13),(3,12),(3,7),(3,6),(3,4),(3,5)

insert into ata.requests (import_schema,import_table) values ('imports','ata_device_by_page')
insert into ata.request_sites (request_id,site_id) values (4,4), (4,5),(4,6)
insert into ata.request_columns (request_id,column_id) values (4,14),(4,12),(4,4),(4,5),(4,6),(4,7)

insert into ata.requests (import_schema,import_table,run_daily,run_monthly,run_yearly,run_seasonally) values ('imports','ata_pages_by_date',1,0,1,0)
insert into ata.request_sites (request_id,site_id) values (5,4),(5,5),(5,6)
insert into ata.request_columns (request_id,column_id) values (5,17),(5,12),(5,5),(5,4),(5,6),(5,19),(5,20),(5,7)


-- query to get all requests
select	r.request_id, r.import_schema, r.import_table, sites.sites site_id,
        r.run_daily, r.run_monthly, r.run_yearly,
        cols.cols
from	ata.requests r
join	(
    select	request_id, string_agg(c.ata_code,',') cols
    from	ata.request_columns rc
    join	ata.columns c
        on	c.column_id=rc.column_id
    group by request_id
) cols
    on	cols.request_id=r.request_id
join	(
    select	request_id, string_agg(c.ata_code,''',''') sites
    from	ata.request_sites rc
    join	ata.sites c
        on	c.site_id=rc.site_id
    group by request_id
) sites
    on	cols.request_id=r.request_id
where	r.is_active=1

-- setup import tables
-- In it's current form, each table requires cut and season.  this could be updated in the script to exclude if required
drop table if exists imports.ata_location_by_page
create table imports.ata_location_by_page (
	geo_country nvarchar(255),
	geo_region nvarchar(255),
	--geo_metro nvarchar(255),
	[page] nvarchar(4000),
	m_visits int,
	m_page_loads int,
	m_unique_visitors int,
	site_id int,
	cut varchar(255),	
	season int
)

drop table if exists imports.ata_time_of_day
create table imports.ata_time_of_day (
	date_day varchar(255),
	device_hour varchar(255),
	m_unique_visitors int,
	site_id int,
	cut varchar(255),	
	season int
)

drop table if exists imports.ata_source_by_page
create table imports.ata_source_by_page (
	src nvarchar(255),
	src_detail nvarchar(1000),
	[page] nvarchar(4000),
	m_visits int,
	m_page_loads int,
	m_unique_visitors int,
	site_id int,
	cut varchar(255),	
	season int
)

drop table if exists imports.ata_device_by_page
create table imports.ata_device_by_page (
	device_type nvarchar(255),
	[page] nvarchar(4000),
	m_visits int,
	m_page_loads int,
	m_unique_visitors int,
	site_id int,
	cut varchar(255),	
	season int
)

drop table if exists imports.ata_pages_by_date
create table imports.ata_pages_by_date (
	[date] date,
	[page] nvarchar(4000),
	m_page_loads int,
	m_visits int,
	m_unique_visitors int,
	m_time_spent_loads int,
	m_bounces int,
	site_id int,
	cut varchar(255),	
	season int
)

/*
select	r.import_table, ata_code, ata_label
from	ata.request_columns rc
left join ata.[columns] c
	on	c.column_id=rc.column_id
join	ata.requests r
	on	r.request_id=rc.request_id

select * from imports.ata_location_by_page
select * from imports.ata_source_by_page
select * from imports.ata_device_by_page
select * from imports.ata_time_of_day where cut='all' order by site_id, date_day, device_hour
select * from imports.ata_pages_by_date*/
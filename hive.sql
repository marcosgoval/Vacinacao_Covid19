--!hdfs dfs -ls  /user/hive/warehouse/covid.db/covid

show databases;

create database covid;

use covid;

show tables;

--drop table covid
--drop table covid_part

create table covid(
regiao string,
estado string,
municipio string,
coduf string,
codmun string,
codRegiaoSaude string,
nomeRegiaoSaude string,
data string,
semanaEpi string, 
populacaoTCU2019 string,
casosAcumulado string,
casosNovos string,
obitosAcumulado string,
obitosNovos string,
Recuperadosnovos string,
emAcompanhamentoNovos string, 
interiorMmetropolitana string)
row format delimited 
fields terminated  by';'
lines terminated by '\n'
stored as textfile
tblproperties("skip.header.line.count"="1")

load data inpath '/user/marcos/data/covid' overwrite  into table covid ;

--select count(*) from covid WHERE municipio <> '';

create table covid_part(
regiao string,
estado string,
--municipio string,
coduf string,
codmun string,
codRegiaoSaude string,
nomeRegiaoSaude string,
data string,
semanaEpi string, 
populacaoTCU2019 string,
casosAcumulado string,
casosNovos string,
obitosAcumulado string,
obitosNovos string,
Recuperadosnovos string,
emAcompanhamentoNovos string, 
interiorMmetropolitana string)
partitioned by (municipio string)
row format delimited 
fields terminated  by';'
lines terminated by '\n'
stored as textfile
tblproperties("skip.header.line.count"="1")

desc covid

desc formatted covid_part;

/*
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.enforce.bucketing =true;*/

insert overwrite table covid_part partition(municipio) select * from covid WHERE municipio <> '';
--where municipio <> '';


--Consultas
select * from covid_part;

select sum(recuperadosnovos) from covid
where recuperadosnovos = 'Ariquemes'

select max((cast recuperadosnovos as int)) from covid where regiao = 'Brasil'

select * from covid where data  = '2021-07-06'

select * from covid  where regiao = 'Brasil' and data = '2022-04-22'

select * from covid  where  estado = "DF" and data = '2022-04-22'

select * from covid limit 10

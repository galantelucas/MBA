/*Linguagem SQL
DDL  - CREATE, ALTER, DROP
*/
use master;
go
--drop database Db_trabalhofinal;
--go
--criacao do banco de dados
CREATE DATABASE Db_trabalhofinal;
go
use Db_trabalhofinal;
go
-- criacao das tabelas e chaves primárias
CREATE TABLE NAVIO (
NAV_Id integer not null identity(1,1) primary key,
NAV_Nome varchar(30) not null unique,
NAV_Capacidade varchar(20) not null,
NAV_Ocupacao varchar(20) CHECK (NAV_Ocupacao in ('Disponivel', 'Indisponivel')) not null,
NAV_data_prevista date not null,
CAR_Id integer null,
POR_Id integer null

);
go

CREATE TABLE CARGA (
CAR_Id integer not null identity(1,1) primary key,
CAR_Peso_kg int not null,
POR_ID integer null,
CAR_Data_chegada date null,
CAR_Status varchar(30) CHECK(CAR_Status in ('Embarcado', 'Não Embarcado')) not null,
NAV_id integer null,
AGE_Id integer null,
CAR_Tipo varchar(25) CHECK (CAR_Tipo in ('Perecivel', 'Sensivel')) not null,
CAR_Validade date null,
CAR_Temperatura varchar(3) null
);
go

CREATE TABLE PORTO (
POR_Id integer not null identity (1,1) primary key,
POR_Cidade varchar(20) not null
);
GO

CREATE TABLE AGENTE (
AGE_Id integer not null identity (1,1) primary key,
AGE_Nome varchar(20) not null,
AGE_Dataexpediente date,
POR_id integer null
);
GO
---------- Alteração das tabelas com as chaves estrangeiras

ALTER TABLE NAVIO
ADD CONSTRAINT FK_navio_porto
foreign key(POR_Id)
references PORTO(Por_Id)
;
go

ALTER TABLE CARGA
ADD CONSTRAINT FK_carga_porto
FOREIGN KEY(POR_Id)
references PORTO(Por_id)
;
go

ALTER TABLE CARGA
ADD CONSTRAINT FK_carga_agente
FOREIGN KEY(AGE_ID)
references AGENTE(AGE_ID);
go

ALTER TABLE AGENTE
ADD CONSTRAINT FK_agente_porto
FOREIGN KEY(POR_Id)
references PORTO(POR_Id);
go

ALTER TABLE CARGA
ADD CONSTRAINT FK_carga_navio
FOREIGN KEY(NAV_Id)
references NAVIO(NAV_Id);
go






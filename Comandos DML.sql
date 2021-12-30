--DML
-- INSERT, UPDATE, DELETE, SELECT, TRUNCATE
USE Db_trabalhofinal;
go
------ Criação tabela navios
-- select * from navio
INSERT INTO NAVIO (NAV_Nome, NAV_Capacidade, NAV_Ocupacao, NAV_data_prevista)
VALUES ('Bravo do mar',
'1000',
'Indisponivel',
'10/12/2021'
)
;
go


INSERT INTO NAVIO (NAV_Nome, NAV_Capacidade, NAV_Ocupacao, NAV_data_prevista)
VALUES 
('Caçador',
'1500',
'Disponivel',
'15/12/2021')

;
go


INSERT INTO NAVIO (NAV_Nome, NAV_Capacidade, NAV_Ocupacao, NAV_data_prevista)
VALUES 
('Mucuripe',
'2000',
'Indisponivel',
'20/12/2021')

;
go


--------- Criação tabela Cargas
-- select * from carga;


insert into CARGA (CAR_Peso_kg, CAR_Data_chegada, CAR_Status, CAR_Tipo, CAR_Validade, CAR_Temperatura)
values (
'700',
'15/12/2021',
'Embarcado',
'Perecivel',
'20/12/2021',
''
		);
go
 

insert into CARGA (CAR_Peso_kg, CAR_Data_chegada, CAR_Status, CAR_Tipo,  CAR_Temperatura)
values (
'800',
'20/12/2021',
'Não Embarcado',
'Sensivel',
'30C'
		);
go

insert into CARGA (CAR_Peso_kg, CAR_Data_chegada, CAR_Status, CAR_Tipo, CAR_Validade, CAR_Temperatura)
values (
'1000',
'15/12/2021',
'Embarcado',
'Perecivel',
'30/12/2021',
''
		);
go


-------------- Criação tabela porto
--Select * from Porto
insert into Porto (POR_Cidade)
VALUES ('Santos');
go

insert into Porto (POR_Cidade)
VALUES ('Rio de Janeiro');
go

insert into Porto (POR_Cidade)
VALUES ('Fortaleza');
go

----------- Criação da tabela AGENTE
-- Select * from agente
insert into Agente (AGE_Nome, AGE_Dataexpediente)
VALUES (
'Fabricio Almeida',
'15/12/2021');
go

insert into Agente (AGE_Nome, AGE_Dataexpediente)
VALUES (
'Vanessa Silva',
'20/12/2021');
go

insert into Agente (AGE_Nome, AGE_Dataexpediente)
VALUES (
'Roberto Carlos',
'10/12/2021');
go

insert into Agente (AGE_Nome, AGE_Dataexpediente)
VALUES (
'francisco santos',
'10/12/2021');
go


--------------Atualizando tabela navio

update navio 
set CAR_ID = '1', por_id = '1'
where nav_id = 1;
go

update navio 
set por_id = '2'
where nav_id = 2;
go

update navio 
set CAR_ID = '3', por_id = '3'
where nav_id = 3;
go



------Atualizando tabela carga


update CARGA
set POR_ID = 1, NAV_Id =1, AGE_ID = 1
where car_id = 1;
go

update CARGA
set POR_ID = 2, AGE_ID = 4
where car_id = 2;
go

update CARGA
set POR_ID = 3, NAV_id = 3, AGE_id = 3
where car_id = 3;
go




------------Atualizando tabela agente

update AGENTE
set por_id = 1
where age_id = 1;
go

update AGENTE
set por_id = 2
where age_id = 2;
go

update AGENTE
set por_id = 3
where age_id = 3;
go

update AGENTE
set por_id = 3
where age_id = 4;
go



/*Linguagem SQL
DQL  - SELECT
*/
use Db_trabalhofinal;
go

------- View de cargas e navios e ordenando por ordem decrescente
CREATE VIEW vw_cargas_navios AS	

select
	navi.NAV_Id
	,navi.NAV_Capacidade
	,navi.NAV_Nome
	,navi.NAV_Ocupacao
	,navi.NAV_data_prevista
	,carg.CAR_Id
	,carg.AGE_Id
	,carg.POR_ID
	,carg.CAR_Data_chegada
	,carg.CAR_Status
	,carg.CAR_Tipo
	,carg.CAR_Peso_kg
	,carg.CAR_Validade
	,carg.CAR_Temperatura
		from NAVIO navi
			inner join CARGA carg
				on navi.NAV_Id = carg.NAV_id;
				go
--Ordenando	
SELECT * FROM vw_cargas_navios
ORDER BY NAV_Id desc;
go


-----------------quantidade de agentes no porto de fortaleza do dia 10/12/2021
select 
a.POR_Cidade
,COUNT(b.AGE_Nome) as qtd_agentes

from PORTO a
	inner join AGENTE b
		ON a.POR_Id = b.POR_id
where b.AGE_Dataexpediente = '2021-12-10'
and a.POR_Cidade like '%fortal%'
		GROUP BY 
a.POR_Cidade
,b.AGE_Dataexpediente
		
			
----------------- inserindo multa por atraso na carga, identificando quem recebeu multa e entregou com atraso
select a.*
	,b.age_nome
	,CASE WHEN DATEDIFF (day, a.nav_data_prevista, a.car_data_chegada) > 0 THEN DATEDIFF (day, a.nav_data_prevista, a.car_data_chegada) * 5.0 ELSE '0' end as MULTA_ATRASO
		from vw_cargas_navios a
			left join agente b
				ON a.age_id = b.age_id
		WHERE DATEDIFF (day, a.nav_data_prevista, a.car_data_chegada) > 0

----------------- contando o peso das cargas e agrupando pelo status

select 
car_status
,sum(car_peso_kg) as soma_do_peso

  from carga 
	group by CAR_status


----------------Descobrir cargas disponiveis e navios disponiveis para embarcar

CREATE VIEW vw_carga_disponivel
as
select
	navi.NAV_Id
	,navi.NAV_Capacidade
	,navi.NAV_Nome
	,navi.NAV_Ocupacao
	,navi.NAV_data_prevista
	,carg.CAR_Id
	,carg.AGE_Id
	,carg.POR_ID
	,carg.CAR_Data_chegada
	,carg.CAR_Status
	,carg.CAR_Tipo
	,carg.CAR_Peso_kg
	,carg.CAR_Validade
	,carg.CAR_Temperatura
	,CASE WHEN navi.NAV_Ocupacao like 'Disponivel' and carg.CAR_Status like 'Não Embarcado' THEN 'Apto a embarcar' Else 'Não apto' END AS 'Operacao'
		from NAVIO navi
			left join CARGA carg
				on navi.POR_Id = carg.POR_ID;
				go











		
		
		
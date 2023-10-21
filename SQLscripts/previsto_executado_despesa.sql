---------------------------------------------------------------------------------------------------
--dashboard.previsto_exec_despesa -----------------------------------------------------------------
---------------------------------------------------------------------------------------------------

truncate table dashboard.previsto_exec_despesa

---------------------------------------------------------------------------------------------------
--Valor previsto mensal (valor anual /12) ---------------------------------------------------------
---------------------------------------------------------------------------------------------------

create table #aux_orcamento_despesa_ini
(
	ano int
	,valor_mensal  float
)

insert into #aux_orcamento_despesa_ini

	SELECT 
		 ano
		,sum(valor_ini)/12		
	FROM hist.orcamento_despesa_ini group by ano;

---------------------------------------------------------------------------------------------------
--Valor executado mensal + Valor executado acumulado mes a mes ------------------------------------
---------------------------------------------------------------------------------------------------

CREATE TABLE #aux_despesa_exec
(
    mes INT,
    ano INT,
    Despesa_Executada FLOAT
);

INSERT INTO #aux_despesa_exec
	SELECT
		mes,
		ano,
		SUM(valor) AS Despesa_Executada
	FROM hist.despesa_exec
	GROUP BY ano, mes
	ORDER BY ano, mes;

CREATE TABLE #aux_cumulative_despesa_exec
(
    mes INT,
    ano INT,
    despesa_exec_c FLOAT
);

INSERT INTO #aux_cumulative_despesa_exec
	SELECT
		mes,
		ano,
		SUM(Despesa_Executada) OVER (PARTITION BY ano ORDER BY ano, mes) AS despesa_exec_c
	FROM #aux_despesa_exec
	ORDER BY ano, mes;

---------------------------------------------------------------------------------------------------
--Despesa Executada x Prevista --------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

insert into dashboard.previsto_exec_despesa
	select 

		A.mes, 
		A.ano, 
		A.Despesa_Executada as [Despesa Executada],
		C.despesa_exec_c as [Despesa Executada Ac.],
		(B.valor_mensal*A.mes) as [Previsto Mensal Ac.], -- Previsto acumulado mes a mes
		[Despesa Prevista Anual] = max(B.valor_mensal*A.mes) over(partition by A.ano) -- Previsto no ano
	from #aux_despesa_exec A left join #aux_orcamento_despesa_ini B on A.ano = B.ano
		left join #aux_cumulative_despesa_exec C on A.ano = C.ano and A.mes = C.mes
				order by A.ano, A.mes

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

drop table #aux_despesa_exec;
drop table #aux_orcamento_despesa_ini;
drop table #aux_cumulative_despesa_exec

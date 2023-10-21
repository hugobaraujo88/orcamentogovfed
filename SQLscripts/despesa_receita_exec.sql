---------------------------------------------------------------------------------------------------
--dashboard.despesa_receita_exec ------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

truncate table dashboard.despesa_receita_exec

---------------------------------------------------------------------------------------------------
--Despesas: Valor executado mensal + Valor executado acumulado mes a mes --------------------------
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

drop table #aux_despesa_exec

---------------------------------------------------------------------------------------------------
--Receitas: Valor executado mensal + Valor executado acumulado mes a mes --------------------------
---------------------------------------------------------------------------------------------------

CREATE TABLE #aux_receitas_real
(
    mes INT,
    ano INT,
    Receita_Executada FLOAT
);

INSERT INTO #aux_receitas_real
	SELECT
		mes,
		ano,
		SUM(valor_receita_real) AS Receita_Executada
	FROM hist.receitas_real
	GROUP BY ano, mes
	ORDER BY ano, mes;

CREATE TABLE #aux_cumulative_receitas_real
(
    mes INT,
    ano INT,
    receita_exec_c FLOAT
);

INSERT INTO #aux_cumulative_receitas_real
	SELECT
		mes,
		ano,
		SUM(Receita_Executada) OVER (PARTITION BY ano ORDER BY ano, mes) AS receita_exec_c
	FROM #aux_receitas_real
	ORDER BY ano, mes;

drop table #aux_receitas_real

---------------------------------------------------------------------------------------------------
--Despesa Executada x Receita Executada -----------------------------------------------------------
---------------------------------------------------------------------------------------------------

insert into dashboard.despesa_receita_exec
	select 
			A.mes, 
			A.ano,
			A.despesa_exec_c as [Despesa Executada],
			B.receita_exec_c as [Receita Executada]
	from #aux_cumulative_despesa_exec A
		left join #aux_cumulative_receitas_real B on A.ano = B.ano
			and A.mes = B.mes order by A.ano, A.mes

			

drop table #aux_cumulative_receitas_real
drop table #aux_cumulative_despesa_exec

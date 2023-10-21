---------------------------------------------------------------------------------------------------
--dashboard.previsto_exec_receita -----------------------------------------------------------------
---------------------------------------------------------------------------------------------------

truncate table dashboard.previsto_exec_receita

---------------------------------------------------------------------------------------------------
--Valor previsto mensal (valor anual /12) ---------------------------------------------------------
---------------------------------------------------------------------------------------------------

create table #aux_receitas_prev
(
	ano int
	,valor_mensal  float
)

insert into #aux_receitas_prev
	SELECT 
		 ano
		,sum(valor_receita_prev)/12		
	FROM hist.receitas_prev group by ano;

---------------------------------------------------------------------------------------------------
--Valor executado mensal + Valor executado acumulado mes a mes ------------------------------------
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

---------------------------------------------------------------------------------------------------
--Despesa Executada x Prevista --------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

insert into dashboard.previsto_exec_receita
	select 
		A.mes, 
		A.ano, 
		A.Receita_Executada as [Receita Executada],
		C.receita_exec_c as [Receita Executada Ac.],
		(B.valor_mensal*A.mes) as [Previsto Mensal Ac.], -- Previsto acumulado mes a mes
		[Receita Prevista Anual] = max(B.valor_mensal*A.mes) over(partition by A.ano) -- Previsto no ano
	from #aux_receitas_real A left join #aux_receitas_prev B on A.ano = B.ano
		left join #aux_cumulative_receitas_real C on A.ano = C.ano and A.mes = C.mes
				order by A.ano, A.mes

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

drop table #aux_receitas_prev
drop table #aux_receitas_real
drop table #aux_cumulative_receitas_real

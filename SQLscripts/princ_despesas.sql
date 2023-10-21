---------------------------------------------------------------------------------------------------
--dashboard.princ_despesas ------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

truncate table dashboard.princ_despesas

---------------------------------------------------------------------------------------------------
--Valor executado mensal por tipo  ----------------------------------------------------------------
---------------------------------------------------------------------------------------------------

create table #aux_princ_despesas

(
    ano INT,
    despesa VARCHAR(255),
    valor_total_ano_despesa FLOAT,
	ranking int
);

INSERT INTO #aux_princ_despesas
	SELECT
			ano, 
			despesa_exec_nome, 
			SUM(valor),
			ranking = row_number() over (partition by ano order by SUM(valor) desc)
		FROM hist.despesa_exec
			group by ano, despesa_exec_nome;

INSERT INTO dashboard.princ_despesas
	 select
		ano,
		despesa,
		valor_total_ano_despesa,
		ranking,
		valor_total_ano_despesa/sum(valor_total_ano_despesa) over (partition by ano)
	from #aux_princ_despesas;


drop table #aux_princ_despesas

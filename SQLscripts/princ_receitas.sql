---------------------------------------------------------------------------------------------------
--dashboard.princ_receitas ------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

truncate table dashboard.princ_receitas

---------------------------------------------------------------------------------------------------
--Valor executado mensal por tipo  ----------------------------------------------------------------
---------------------------------------------------------------------------------------------------

create table #aux_princ_receitas

(
    ano INT,
    receita VARCHAR(255),
    valor_total_ano_receita FLOAT,
	ranking int
);

INSERT INTO #aux_princ_receitas
	SELECT
			ano, 
			receita_nome, 
			SUM(valor_receita_real),
			ranking = row_number() over (partition by A.ano order by SUM(valor_receita_real) desc)
		FROM hist.receitas_real A
			group by A.ano, A.receita_nome

INSERT INTO dashboard.princ_receitas
	 select
		ano,
		receita,
		valor_total_ano_receita,
		ranking,
		valor_total_ano_receita/sum(valor_total_ano_receita) over (partition by ano)
	from #aux_princ_receitas

drop table #aux_princ_receitas

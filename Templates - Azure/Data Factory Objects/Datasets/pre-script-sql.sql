@{
    if(
        equals(item().name, 'despesa_exec'), 
        concat('DELETE FROM transparencia.', item().name, ' where ano = ', variables('ano'), ' and mes = ', variables('mes')),
        if(contains(item().name, 'receitas'),
           concat('DELETE FROM transparencia.', item().name, ' where ano = ', variables('ano')),
           concat('DELETE FROM transparencia.', item().name, ' where ano = ', variables('ano'))))
           
}

@{
    if(
        equals(item().name, 'despesa_exec'), 
        concat('DELETE FROM transparencia.', item().name, ' where ano = ', variables('ano'), ' and mes = ', variables('mes')),
        concat('DELETE FROM transparencia.', item().name, ' where ano = ', variables('ano'))
    )          
}

@{if(equals(item().name, 'despesa_exec'),concat('DELETE FROM transparencia.', item().name, ' where ano = ', formatDateTime(utcNow(),'yyyy'), ' and mes = ', formatDateTime(subtractFromTime(utcnow(), 1, 'Month'),'MM')),if(contains(item().name, 'receitas'),concat('DELETE FROM transparencia. ', item().name, ' where ano = ', formatDateTime(utcNow(),'yyyy'), ' and mes = ', formatDateTime(subtractFromTime(utcnow(), 1, 'Month'),'MM')),concat('DELETE FROM transparencia.', item().name, ' where ano = ', formatDateTime(utcNow(),'yyyy'))))}
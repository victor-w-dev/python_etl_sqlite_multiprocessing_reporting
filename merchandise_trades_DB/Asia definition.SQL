with T as (select acode, ccode, desc from areacnty 
left join country on areacnty.CCODE = country.code
where acode = 600
order by desc)

select T.acode,area.desc,T.ccode, T.desc from T left join area on T.acode = area.areacode
select comedien_ft, original_filename from cm.cm_comediens comediens 
inner join cm.cm_medias medias on comediens.id_comedien = medias.id_comedien;

select table_name, column_name from information_schema.columns
where table_schema = 'cm' order by table_name,ordinal_position;
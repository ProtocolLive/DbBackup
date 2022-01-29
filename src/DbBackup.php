<?php
//Protocol Corporation Ltda.
//https://github.com/ProtocolLive/PhpLive/
//Version 2022.01.29.00
//For PHP >= 8

class PhpLiveDbBackup{
  private ?PhpLivePdo $PhpLivePdo = null;
  private array $Delete = [];
  private string $File;
  private ZipArchive $Zip;

  public function __construct(PhpLivePdo &$PhpLivePdo){
    $this->PhpLivePdo = $PhpLivePdo;
  }

  public function Tables(
    string $Folder = __DIR__ . '/sql',
    int $Progress = 1,
    string $TranslateTables = 'tables',
    string $TranslateFk = 'foreign keys'
  ):string|false{
    if($this->PhpLivePdo === null):
      return false;
    endif;
    $this->ZipOpen($Folder, 0);
    $tables = $this->PhpLivePdo->RunCustom(
      "show tables like '%'",
      OnlyFieldsName: false
    );
    if($Progress != 0):
      $TablesCount = count($tables);
      $TablesLeft = 0;
      printf('%d %s<br>0%%<br>', $TablesCount, $TranslateTables);
    endif;

    $file = fopen($Folder . '/tables.sql', 'w');
    foreach($tables as $table):
      $cols = $this->PhpLivePdo->RunCustom("
        select COLUMN_NAME,
          DATA_TYPE,
          CHARACTER_MAXIMUM_LENGTH,
          NUMERIC_PRECISION,
          NUMERIC_SCALE,
          IS_NULLABLE,
          COLUMN_DEFAULT,
          COLUMN_TYPE,
          EXTRA,
          COLUMN_KEY,
          COLLATION_NAME
        from information_schema.columns
        where table_name='" . $table[0] . "'
        order by ordinal_position
      ");
      $line = 'create table ' . $table[0] . "(\n";
      foreach($cols as $col):
        $line .= '  ' . $this->PhpLivePdo->Reserved($col['COLUMN_NAME']) . ' ' . $col['DATA_TYPE'];
        //Field size for integers is deprecated
        if($col['DATA_TYPE'] == 'varchar'):
          $line .= '(' . $col['CHARACTER_MAXIMUM_LENGTH'] . ')';
          $line .= ' collate ' . $col['COLLATION_NAME'];
        elseif($col['DATA_TYPE'] == 'decimal'):
          $line .= '(' . $col['NUMERIC_PRECISION'] . ',' . $col['NUMERIC_SCALE'] . ')';
        endif;
        //Unsigned for decimal is deprecated
        if(strpos($col['COLUMN_TYPE'], 'unsigned') !== false and $col['DATA_TYPE'] != 'decimal'):
          $line .= ' unsigned';
        endif;
        if($col['IS_NULLABLE'] == 'NO'):
          $line .= ' not null';
        endif;
        if($col['COLUMN_DEFAULT'] != null and $col['COLUMN_DEFAULT'] != 'NULL'):
          $line .= ' default ';
          if($col['DATA_TYPE'] == 'varchar'):
            $line .= "'" . $col['COLUMN_DEFAULT'] . "'";
          else:
            $line .= $col['COLUMN_DEFAULT'];
          endif;
        endif;
        if($col['EXTRA'] == 'auto_increment'):
          $line .= ' auto_increment';
        endif;
        if($col['COLUMN_KEY'] == 'PRI'):
          $line .= ' primary key';
        elseif($col['COLUMN_KEY'] == 'UNI'):
          $line .= ' unique key';
        endif;
        $line .= ",\n";
      endforeach;
      fwrite($file, substr($line, 0, -2) . "\n) ");
      $table = $this->PhpLivePdo->RunCustom("
        select
          ENGINE,
          TABLE_COLLATION
        from information_schema.tables
        where table_name='" . $table[0] . "'
      ");
      fwrite($file, 'engine=' . $table[0]['ENGINE'] . ' ');
      fwrite($file, 'collate=' . $table[0]['TABLE_COLLATION'] . ";\n\n");
      if($Progress != 0):
        printf('%d%%<br>', ++$TablesLeft * 100 / $TablesCount);
      endif;
    endforeach;
    //foreign keys
    $cols = $this->PhpLivePdo->RunCustom('
      select
        rc.table_name,
        constraint_name,
        column_name,
        rc.referenced_table_name,
        referenced_column_name,
        delete_rule,
        update_rule
      from
        information_schema.referential_constraints rc
          left join information_schema.key_column_usage using(constraint_name)
      order by rc.table_name
    ');
    $TablesCount = count($cols);
    if($Progress != 0):
      $TablesLeft = 0;
      printf('%d %s<br>0%%<br>', $TablesCount, $TranslateFk);
    endif;
    foreach($cols as $col):
      $line = 'alter table ' . $col['table_name'] . "\n";
      $line .= '  add constraint ' . $col['constraint_name'];
      $line .= ' foreign key(' . $col['column_name'] . ') references ';
      $line .= $col['referenced_table_name'] . '(' . $col['referenced_column_name'] . ') ';
      $line .= 'on delete ' . strtolower($col['delete_rule']) . ' on update ' . strtolower($col['update_rule']) . ",\n";
      fwrite($file, substr($line, 0, -2) . ";\n\n");
      if($Progress != 0):
        printf('%d%%<br>', ++$TablesLeft * 100 / $TablesCount);
      endif;
    endforeach;
    fclose($file);
    $this->Zip->addFile($Folder . '/tables.sql', 'tables.sql');
    $this->ZipClose();
    unlink($Folder . '/tables.sql');
    return substr($_SERVER['REQUEST_URI'], 0, strrpos($_SERVER['REQUEST_URI'], '/')) . $this->File;
  }

  public function Data(
    string $Folder = __DIR__ . '/sql',
    int $Progress = 2,
    string $TranslateTables = 'tables',
    string $TranslateRows = 'rows'
  ):string{
    if($this->PhpLivePdo === null):
      return false;
    endif;

    $last = null;
    $this->ZipOpen($Folder, 1);
    $tables = $this->PhpLivePdo->RunCustom(
      "show tables like '%'",
      OnlyFieldsName: false
    );
    if($Progress != 0):
      $TablesCount = count($tables);
      $TablesLeft = 0;
      printf('%d %s<br><br>0%%<br>', $TablesCount, $TranslateTables);
    endif;
    foreach($tables as $table):
      $this->PhpLivePdo->RunCustom('lock table ' . $table[0] . ' write');

      $rows = $this->PhpLivePdo->RunCustom('select * from ' . $table[0]);
      $RowsCount = count($rows);
      $RowsLeft = 0;
      if($Progress == 2):
        printf('%s (%d %s)<br>', $table[0], $RowsCount, $TranslateRows);
      endif;
      if($RowsCount > 0):
        $file = fopen($Folder . $table[0] . '.sql', 'w');
        $this->Delete[] = $Folder . $table[0] . '.sql';

        $temp = $this->PhpLivePdo->RunCustom('checksum table ' . $table[0]);
        fwrite($file, '-- Table checksum ' . $temp[0]['Checksum'] . "\n\n");

        foreach($rows as $row):
          $cols = '';
          $values = '';
          foreach($row as $col => $value):
            $cols .= $this->PhpLivePdo->Reserved($col) . ',';
            if($value === null):
              $values .= 'null,';
            else:
              $values .= "'" . str_replace("'", "''", $value) . "',";
            endif;
          endforeach;
          $cols = substr($cols, 0, -1);
          $values = substr($values, 0, -1);
          fwrite($file, 'insert into ' . $table[0] . '(' . $cols . ') values(' . $values . ");\n");
          if($Progress == 2):
            $percent = ++$RowsLeft * 100 / $RowsCount;
            if(($percent % 25) == 0 and floor($percent) !== $last):
              printf('%d%%...', $percent);
              $last = floor($percent);
            endif;
          endif;
        endforeach;
        $this->PhpLivePdo->RunCustom('unlock tables');
        fclose($file);
        $this->Zip->addFile($Folder . $table[0] . '.sql', $table[0] . '.sql');
      endif;
      if($Progress != 0):
        printf('<br><br>%d%%<br>', ++$TablesLeft * 100 / $TablesCount);
      endif;
    endforeach;
    $this->ZipClose();
    $this->Delete();
    return substr($_SERVER['REQUEST_URI'], 0, strrpos($_SERVER['REQUEST_URI'], '/')) . $this->File;
  }

  private function ZipOpen(string $Folder, int $Type):void{
    if(file_exists($Folder) == false):
      mkdir($Folder, 0755);
    endif;
    $this->Zip = new ZipArchive();
    $this->File = date('Y-m-d-H-i-s-');
    if($Type == 0):
      $this->File .= 'tables';
    else:
      $this->File .= 'data';
    endif;
    $this->File .= '.zip';
    $this->Zip->open($Folder . '/' . $this->File, ZipArchive::CREATE);
  }

  private function ZipClose():void{
    $this->Zip->close();
  }

  private function Delete():void{
    foreach($this->Delete as $file):
      unlink($file);
    endforeach;
  }
}
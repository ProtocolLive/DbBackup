<?php
//Protocol Corporation Ltda.
//https://github.com/ProtocolLive/PhpLive/

namespace ProtocolLive\DbBackup;
use ProtocolLive\PhpLiveDb\PhpLiveDb;
use PDO;
use ZipArchive;

/**
 * @version 2024.01.15.00
 */
final class DbBackup{
  private array $Delete = [];
  private string $File;
  private ZipArchive $Zip;

  public function __construct(
    private PhpLiveDb $PhpLiveDb,
    private string $Password,
    private string $Folder = __DIR__ . '/sql',
    private bool $Progress = true,
    private string $TranslateTable = 'Table',
    private string $TranslateTables = 'tables',
    private string $TranslateRows = 'rows',
    private string $TranslateFk = 'foreign keys',
    private string $TranslateDone = 'Done  ',
    private string $TranslateDoneAll = 'All done',
    private string $TranslateProgress = 'Progress'
  ){
    if(PHP_SAPI === 'cli'):
      ini_set('display_errors', '1');
      restore_error_handler();
      restore_exception_handler();
    endif;
    $_SERVER['argv'] ??= [];
    if(in_array('-CronNoEmail', $_SERVER['argv'])):
      $this->Progress = false;
    endif;
  }

  public function Data():string|null{
    $this->ZipOpen($this->Folder, 1);
    $pdo = $this->PhpLiveDb->GetCustom();
    $stm = $pdo->query("show tables like '%'");
    $tables = $stm->fetchAll(PDO::FETCH_BOTH);
    if($this->Progress):
      $TablesCount = count($tables);
      $TablesLeft = 0;
      echo $TablesCount . ' ' . $this->TranslateTables . ':' . PHP_EOL;
    endif;
    foreach($tables as $table):
      if($this->Progress):
        $temp = $TablesLeft++ * 100 / $TablesCount;
        if($temp > 0):
          echo $this->TranslateProgress . ': ' . number_format($temp, 2) . '%' . PHP_EOL;
        endif;
      endif;
      $pdo->exec('lock table ' . $table[0] . ' write');

      $consult = $this->PhpLiveDb->Select($table[0]);
      $rows = $consult->Run();
      $RowsCount = count($rows);
      $RowsLeft = 0;
      if($this->Progress):
        echo $this->TranslateTable . ' ' . $table[0] . ' (' . $RowsCount . ' ' . $this->TranslateRows . ')' . PHP_EOL;
      endif;
      if($this->Progress and $RowsCount === 0):
        echo $this->TranslateDone;
      else:
        $file = fopen($this->Folder . $table[0] . '.sql', 'w');
        $this->Delete[] = $this->Folder . $table[0] . '.sql';

        $stm = $pdo->query('checksum table ' . $table[0]);
        $temp = $stm->fetchColumn(1);
        fwrite($file, '-- Table checksum ' . $temp . PHP_EOL . PHP_EOL);

        foreach($rows as $row):
          $cols = '';
          $values = '';
          foreach($row as $col => $value):
            $cols .= $this->PhpLiveDb->Reserved($col) . ',';
            if($value === null):
              $values .= 'null,';
            else:
              $values .= "'" . str_replace("'", "''", $value) . "',";
            endif;
          endforeach;
          $cols = substr($cols, 0, -1);
          $values = substr($values, 0, -1);
          fwrite($file, 'insert into ' . $table[0] . '(' . $cols . ') values(' . $values . ');' . PHP_EOL);
          if($this->Progress and PHP_SAPI === 'cli'):
            $temp = ++$RowsLeft * 100 / $RowsCount;
            if($temp === 100):
              echo "\r" . $this->TranslateDone;
            else:
              echo "\r" . number_format($temp, 2) . '%';
            endif;
          endif;
        endforeach;
        $pdo->exec('unlock tables');
        fclose($file);
        $this->Zip->addFile($this->Folder . $table[0] . '.sql', $table[0] . '.sql');
        $this->Zip->setEncryptionName($table[0] . '.sql', ZipArchive::EM_AES_256);
      endif;
      if($this->Progress):
        echo PHP_EOL;
      endif;
    endforeach;
    if($this->Progress):
      echo $this->TranslateDoneAll . PHP_EOL;
    endif;
    $this->ZipClose();
    $this->Delete();
    if(is_file($this->Folder . '/' . $this->File) === false):
      echo 'Error' . PHP_EOL;
      exit(1);
    endif;
    if(PHP_SAPI === 'cli'):
      return null;
    else:
      return substr($_SERVER['SERVER_NAME'], 0, strrpos($_SERVER['SERVER_NAME'], '/')) . $this->File;
    endif;
  }

  private function Delete():void{
    foreach($this->Delete as $file):
      unlink($file);
    endforeach;
  }

  public function Tables():string|null{
    $this->ZipOpen($this->Folder, 0);
    $pdo = $this->PhpLiveDb->GetCustom();
    $stm = $pdo->prepare("show full tables where Table_Type='base table'");
    $stm->execute();
    $tables = $stm->fetchAll(PDO::FETCH_BOTH);
    if($this->Progress):
      $TablesCount = count($tables);
      $TablesLeft = 0;
      echo $TablesCount . ' ' . $this->TranslateTables . ':' . PHP_EOL . '0%';
    endif;

    $file = fopen($this->Folder . '/tables.sql', 'w');
    foreach($tables as $table):
      $stm = $pdo->prepare("
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
      $stm->execute();
      $cols = $stm->fetchAll(PDO::FETCH_ASSOC);
      $line = 'create table ' . $table[0] . '(' . PHP_EOL;
      foreach($cols as $col):
        $line .= '  ' . $this->PhpLiveDb->Reserved($col['COLUMN_NAME']) . ' ' . $col['DATA_TYPE'];
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
        $line .= ',' . PHP_EOL;
      endforeach;
      fwrite($file, substr($line, 0, -2) . PHP_EOL . ') ');
      $stm = $pdo->prepare('
        select
          ENGINE,
          TABLE_COLLATION
        from information_schema.tables
        where table_name=:table
      ');
      $stm->bindValue(':table', $table[0], PDO::PARAM_STR);
      $stm->execute();
      $table = $stm->fetchAll(PDO::FETCH_ASSOC);
      fwrite($file, 'engine=' . $table[0]['ENGINE'] . ' ');
      fwrite($file, 'collate=' . $table[0]['TABLE_COLLATION'] . ';' . PHP_EOL . PHP_EOL);
      if($this->Progress):
        $temp = ++$TablesLeft * 100 / $TablesCount;
        echo "\r";
        if($temp === 100):
          echo $this->TranslateDone;
        else:
          echo number_format($temp, 2) . '%';
        endif;
      endif;
    endforeach;
    if($this->Progress):
      echo PHP_EOL;
    endif;
    //foreign keys
    $stm = $pdo->prepare('
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
    $stm->execute();
    $cols = $stm->fetchAll(PDO::FETCH_ASSOC);
    $TablesCount = count($cols);
    if($this->Progress):
      $TablesLeft = 0;
      echo $TablesCount . ' ' . $this->TranslateFk . ':' . PHP_EOL . '0%';
    endif;
    foreach($cols as $col):
      $line = 'alter table ' . $col['table_name'] . PHP_EOL;
      $line .= '  add constraint ' . $col['constraint_name'];
      $line .= ' foreign key(' . $col['column_name'] . ') references ';
      $line .= $col['referenced_table_name'] . '(' . $col['referenced_column_name'] . ') ';
      $line .= 'on delete ' . strtolower($col['delete_rule']) . ' on update ' . strtolower($col['update_rule']) . ',' . PHP_EOL;
      fwrite($file, substr($line, 0, -2) . ';' . PHP_EOL . PHP_EOL);
      if($this->Progress and PHP_SAPI === 'cli'):
        $temp = ++$TablesLeft * 100 / $TablesCount;
        echo "\r";
        if($temp === 100):
          echo $this->TranslateDone;
        else:
          echo number_format($temp, 2) . '%';
        endif;
      endif;
    endforeach;
    if($this->Progress):
      echo PHP_EOL;
    endif;
    fclose($file);
    $this->Zip->addFile($this->Folder . '/tables.sql', 'tables.sql');
    $this->Zip->setEncryptionName('tables.sql', ZipArchive::EM_AES_256);
    $this->ZipClose();
    unlink($this->Folder . '/tables.sql');
    if(is_file($this->Folder . '/' . $this->File) === false):
      echo 'Error' . PHP_EOL;
      exit(1);
    endif;
    if($this->Progress):
      echo $this->TranslateDoneAll . PHP_EOL;
    endif;
    if(PHP_SAPI === 'cli'):
      return null;
    else:
      return substr($_SERVER['SERVER_NAME'], 0, strrpos($_SERVER['SERVER_NAME'], '/')) . $this->File;
    endif;
  }

  private function ZipOpen(
    string $Folder,
    int $Type
  ):void{
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
    $this->Zip->setPassword($this->Password);
    echo 'Saving in ' . $Folder . '/' . $this->File . PHP_EOL;
  }

  private function ZipClose():void{
    $this->Zip->close();
  }
}
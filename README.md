# **LaraCassandra**

A Laraval database driver for Cassandra.

## **Installation**

Before installation:

This is project is still on development progress, before add this on your project, please be sure minimum-stability variable is "dev" 

> "minimum-stability": "dev",

Install using composer:

> composer require torecan/lara-cassandra

To support the Laravel database migration feature a custom migration service provider is needed:

- LaraCassandra\CassandraMigrationServiceProvider::class

It must be added at the very top of the service provider list so it can correctly override the default migration service provider.

## **Configuration**

Change your default database connection name in config/database.php:

    'default' => env('DB_CONNECTION', 'cassandra'),

And add a new cassandra connection:

    'cassandra' => [
        'driver' => 'cassandra',
        'host' => env('DB_HOST', '127.0.0.1'),
        'port' => env('DB_PORT', 8082),
        'keyspace' => env('DB_DATABASE', 'cassandra_db'),
        'username' => env('DB_USERNAME', ''),
        'password' => env('DB_PASSWORD', ''),
        'page_size'       => env('DB_PAGE_SIZE', 5000),
        'consistency'     => LaraCassandra\Consistency::LOCAL_ONE,
        'timeout'         => null,
        'connect_timeout' => 5.0,
        'request_timeout' => 12.0,
    ],

### .env Examples
```
  DB_CONNECTION=cassandra
  DB_HOST=127.0.0.1 
  DB_PORT=8082
```
or
```
  DB_CONNECTION=cassandra
  DB_HOST=172.198.1.1,172.198.1.2,172.198.1.3
  DB_PORT=8082,8082,7748

  DB_DATABASE=db_name
  
  DB_USERNAME=torecan
  DB_PASSWORD=***
  
  DB_PAGE_SIZE=500
```

### Supported Consistency Settings

  - LaraCassandra\Consistency::ALL
  - LaraCassandra\Consistency::ANY
  - LaraCassandra\Consistency::EACH_QUORUM
  - LaraCassandra\Consistency::LOCAL_ONE
  - LaraCassandra\Consistency::LOCAL_QUORUM
  - LaraCassandra\Consistency::LOCAL_SERIAL
  - LaraCassandra\Consistency::ONE
  - LaraCassandra\Consistency::TWO
  - LaraCassandra\Consistency::THREE
  - LaraCassandra\Consistency::QUORUM
  - LaraCassandra\Consistency::SERIAL

## **Schema**

Laravel migration features are supported (when LaraCassandra\CassandraMigrationServiceProvider is used):

  > php artisan migrate

  > php artisan make:migration createNewTable

## **Examples**
See
  - https://laravel.com/docs/11.x/database
  - https://laravel.com/docs/11.x/eloquent
 
Not all features are supported by Cassandra - those will throw exceptions when used.

Additionaly these feautres are supported by this driver:

- Connection and Builder classes support setting the query consistency via `setConsistency()`, for example:
  ```
    DB::table('example')->setConsistency(Consistency::ALL)->where('id', 1)->get();
  ```
- Builder classes support allow filtering via `allowFiltering()`, for example:
  ```
    DB::table('example')->where('time', '>=', 1)->allowFiltering()->get();
  ```

## **Auth**

! TODO !

### This project is forked from https://github.com/cubettech/lacassa

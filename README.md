s3cp
====

[![Build Status](https://drone.io/github.com/masahide/s3cp/status.png)](https://drone.io/github.com/masahide/s3cp/latest)

* S3へrsyncのようにディレクトリ丸ごとアップロードします
* アップロードの際、並列で複数のファイルを同時にアップロードすることが可能です
* 既にアップロード済みのファイルがある場合は、ファイルサイズもしくはmd5sum(オプションで指定可)で検証し、異なる場合は上書きでアップロードします
* 20MB以上のファイルは分割し [マルチパートアップロード](http://docs.aws.amazon.com/ja_jp/AmazonS3/latest/dev/uploadobjusingmpu.html) を並列で行います
* アップロードの中断・再開に対応(20MB以上のファイルのアップロード時は処理のエラー等による中断またはctrl+c等の強制中断を行った後、再度アップロードを実行した場合はアップロード済みパートはスキップする)

Download
--------

https://drone.io/github.com/masahide/s3cp/files


注意
----

* S3からのダウンロード機能は未実装です
* シンボリックリンクは追跡します(循環参照無限ループを回避するため、symlinkは20階層でストップします)
* Windows未対応




セキュリティ認証情報
--------------------


AWSの認証情報(Access keyやSecret access keyなど)の指定は、以下の3つの方法に対応しており、
s3cpは自動でこの順番に認証方式を試みます

* 環境変数にセット

```bash:
export AWS_ACCESS_KEY_ID="hoge"
export AWS_SECRET_ACCESS_KEY="fuga"
````

* IAM rolesをインスタンスに設定する
詳細はこちらに
http://docs.aws.amazon.com/ja_jp/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

* Credential file (~/.aws/credentials) の[default]に設定する
詳細はこちらに
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-config-files



使い方
------

単一ファイルのアップロードの場合:

```bash:
$ s3cp [options] <ローカルのファイルパス> <バケット名> <アップロード先S3のディレクトリパス>
$ s3cp [options] <ローカルのファイルパス> <バケット名> <アップロードS3のファイル名(フルパス)>
```

ディレクトリの丸ごとアップロードの場合

```
$ s3cp -r [options] <ローカルのディレクトリパス> <バケット名> <S3のディレクトリパス>
```

### 例:

```
$ s3cp hoge.html test-bucket html/
```
`test-bucket`バケットに `html/hoge.html`として保存されます

```
$ s3cp hoge.html test-bucket html/fuga.html
```
`test-bucket`バケットに `html/fuga.html`として保存されます


```
$ s3cp -r /var/tmp/piyo test-bucket html/fuge
```
`/var/tmp/piyo`ディレクトリを `test-bucket`バケットの `html/fuge/` ディレクトリとしてコピーします



### options:

 *  -r
   *  ディレクトリコピーモード
 * -checkmd5=false:
   * 同名のファイルが既に存在する場合にMD5sumを検証し、異なる場合のみ上書
 * -checksize=true:
   * 同名のファイルが既に存在する場合にファイルサイズを検証し、異なる場合のみ上書
 * -n=1:
   * 並列アップロードする数(デフォルト:1)
 * -region=ap-northeast-1:
   * 対象リージョンの指定
 *  -jsonLog
   * 出力形式をjsonに
 * -ACL
   * ACLを指定します。 default:private  (public-read,public-read-write,authenticated-read,bucket-owner-full-control,bucket-owner-read)
 *  -version
   * versionの表示
 *  -d=0: log level
   * ログ出力レベルの指定。0から5までで5が最大限に情報を出力します
 * 以下はリトライのルールを設定します
   *  -RetryInitialInterval=500: Retry Initial Interval (Millisecond)
   *  -RetryMaxElapsedTime=15: Retry Max Elapsed Time (Minute)
   *  -RetryMaxInterval=60: Retry Max Interval (Second)
   *  -RetryMultiplier=1.5: Retry Multiplier
   *  -RetryRandomizationFactor=0.5: Retry Randomization Factor


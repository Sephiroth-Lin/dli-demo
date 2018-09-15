# Gene SDK指南

## 解压缩工具包
```bash
tar xvzf GatkServerlessTools.tar.gz
```

## 进入脚本所在路径
```bash
cd ./GatkServerlessTools
```

## 编辑当前路径下的配置文件 gene_config.property

```bash
### 华为公有云IAM服务地址
iam_endpoint = https://iam.cn-north-1.myhuaweicloud.com/v3/auth/tokens

### DLI服务地址
dli_endpoint = https://dli.cn-north-1.myhuaweicloud.com/v2.0/

### 用户名(公有云账号)
user_name =

### 密码
password =

### 用户的项目ID(在我的凭证里面,查询用户华北区的项目Id)
project_id =

### 基因作业输入文件类型,支持 fastq 和 bam 两种
file_type = fastq

### 参考基因,支持 hg19 和 hg38 两种
ref = hg19

### 已知变异基因集,以逗号分隔
### 参考基因为 hg19 时,支持 1000g_phase1.indels.hg19.sites.vcf/dbsnp_138.hg19.vcf/1000g_omni2.5.hg19.sites.vcf/1000g_phase1.snps.high_confidence.hg19.sites.vcf/mills_and_1000g_gold_standard.indels.hg19.sites.vcf/hapmap_3.3.hg19.sites.vcf 六种
### 参考基因为 hg38 时,支持 1000g_omni2.5.hg38.vcf/1000g_phase1.snps.high_confidence.hg38.vcf/dbsnp_144.hg38.vcf/hapmap_3.3.hg38.vcf/homo_sapiens_assembly38.known_indels.vcf/mills_and_1000g_gold_standard.indels.hg38.vcf 六种
known_sites = 1000g_phase1.indels.hg19.sites.vcf,1000g_phase1.snps.high_confidence.hg19.sites.vcf

### 运行参数,参数为冒号分隔的key:value形式,参数之间以逗号分隔(可选)
conf = PL:illumina, SM:SM1, MR:100000

### 是否导出对比文件,默认为False(可选)
is_export_bam =

### 对比文件导出路径,is_export_bam为True时有效(可选)
bam_output_path =

### 是否输出gvcf,默认为False(可选)
is_output_gvcf =

```

## 执行脚本
### 1. 提交基因作业(样例数据 ERR000589)
```bash
# -I： 输入文件在存储桶中的路径,路径之间以逗号分隔
# -O:  指定输出vcf文件的路径
python gene_job.py SubmitJob -I s3a://notebook/ERR000589_fastq/ERR000589_1.filt.fastq,s3a://notebook/ERR000589_fastq/ERR000589_2.filt.fastq -O s3a://dli-gen/ERR000589_fastq/ERR000589_output.vcf
```

### 2. 查询基因作业列表
```bash
python gene_job.py ListJob
```

# Gatk Serverless SDK指南

## 解压缩工具包
```bash
tar xvzf GatkServerlessTools.tar.gz
```

## 进入脚本所在路径
```bash
cd ./GatkServerlessTools
```

## 编辑当前路径下的配置文件 config.property

```bash
### 华为公有云IAM服务地址
iam_endpoint = https://iam.cn-north-1.myhuaweicloud.com/v3/auth/tokens

### DLI服务地址
dli_endpoint = https://dli.cn-north-1.myhuaweicloud.com/v2.0/

### 用户名(公有云账号)
user_name = 

### 密码
password = 

### 用户的项目ID(在我的凭证里面,查询用户华北区的项目Id)
project_id = 

### 用户在DLI上创建的cluster_name
cluster_name = 

### 标准基因组的OBS地址(2bit格式)
ref_path = s3a://notebook/wsg_practice/ref/ref.2bit

### 已知基因突变数据库的OBS地址,多个knownsite文件路径之间用逗号分隔
knownsite_paths = s3a://notebook/wsg_practice/ref/dbsnp.vcf,s3a://obs-e9ce/gatk_bundle_2.8_hg19/Mills_and_1000G_gold_standard.indels.hg19.sites.vcf

### 任务检查时间
task_status_check_period = 15

### 执行FastQToBam步骤使用的集群(目前和cluster_name一样)
picard_cluster_name = 
```

## 执行脚本
### 1. 输入类型为bam(样例数据 mother.bam)
```bash
# -i： 输入bam文件在存储桶中的路径
# -o:  指定输出vcf文件的路径
# -t: 输入文件类型
python gatk_workflow.py -t bam -i s3a://notebook/wsg_practice/input/mother.bam -o s3a://notebook/wsg_practice/output/mother.sort_output.vcf
```

### 2. 输入类型为fastq(样例数据 ERR000589)
```bash
# --F1： 输入fastq1文件在存储桶中的路径
# --F2： 输入fastq2文件在存储桶中的路径
# -o:  指定输出vcf文件的路径
# -t: 输入文件类型
python gatk_workflow.py -t fastq --F1 s3a://notebook/ERR000589_fastq/ERR000589_1.filt.fastq --F2 s3a://notebook/ERR000589_fastq/ERR000589_2.filt.fastq -o s3a://dli-gen/ERR000589_fastq/ERR000589_output.vcf --SM SM1 --PL illumina
```

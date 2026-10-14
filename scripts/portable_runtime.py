"""Discover the executing Python/Java runtime without machine-specific venv paths."""
import importlib.metadata,json,os,platform,re,shutil,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def configure_runtime():
    if sys.version_info[:2]!=(3,11):raise RuntimeError('Use Python 3.11; create a venv with that interpreter')
    if importlib.metadata.version('pyspark')!='3.5.7':raise RuntimeError('Install requirements-spark.txt: PySpark 3.5.7 is required')
    if importlib.metadata.version('py4j')!='0.10.9.7':raise RuntimeError('Install requirements-spark.txt: Py4J 0.10.9.7 is required')
    import pyspark
    exe='java.exe' if os.name=='nt' else 'java'
    homes=[]
    if os.environ.get('JAVA_HOME'):homes.append(Path(os.environ['JAVA_HOME']))
    manifest=ROOT/'.runtime/java_manifest.json'
    if manifest.exists():
        data=json.loads(manifest.read_text(encoding='utf-8'));homes.append(ROOT/data['java_home_relative'])
    for candidate in sorted((ROOT/'.runtime/java17').glob('*')):
        homes.extend([candidate/'Contents/Home',candidate])
    java=next((str(h/'bin'/exe) for h in homes if (h/'bin'/exe).is_file()),None) or shutil.which('java')
    if not java:raise RuntimeError('Install Java 17 and set JAVA_HOME or put Java 17 on PATH')
    response=subprocess.run([java,'-XshowSettings:properties','-version'],capture_output=True,text=True,timeout=30)
    text=response.stdout+'\n'+response.stderr
    home=re.search(r'^\s*java.home\s*=\s*(.+)$',text,re.M);version=re.search(r'^\s*java.version\s*=\s*(.+)$',text,re.M)
    if response.returncode or not home or not version or version[1].strip().split('.')[0]!='17':raise RuntimeError('The discovered Java must be Java 17; check JAVA_HOME/PATH')
    java_home=Path(home[1].strip());os.environ['JAVA_HOME']=str(java_home)
    os.environ['PATH']=str(java_home/'bin')+os.pathsep+os.environ.get('PATH','')
    os.environ.update(SPARK_HOME=str(Path(pyspark.__file__).resolve().parent),PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable,SPARK_LOCAL_IP='127.0.0.1',PYTHONIOENCODING='utf-8')
    if os.name=='nt':
        hadoop=os.environ.get('HADOOP_HOME');hfile=ROOT/'.runtime/hadoop_manifest.json'
        if not hadoop and hfile.exists():hadoop=str(ROOT/json.loads(hfile.read_text(encoding='utf-8'))['hadoop_home_relative'])
        if not hadoop or not all((Path(hadoop)/'bin'/n).is_file() for n in ['winutils.exe','hadoop.dll']):raise RuntimeError('Windows needs verified Hadoop native components: run python scripts/setup_hadoop_windows.py')
        os.environ['HADOOP_HOME']=hadoop;os.environ['PATH']=str(Path(hadoop)/'bin')+os.pathsep+os.environ['PATH']
    return {'python_executable':sys.executable,'python':platform.python_version(),'pyspark':pyspark.__version__,'py4j':importlib.metadata.version('py4j'),'java':version[1].strip(),'java_home':str(java_home),'os':platform.platform(),'architecture':platform.machine(),'timezone_contract':'Asia/Singapore'}

if __name__=='__main__':print(json.dumps(configure_runtime(),indent=2))

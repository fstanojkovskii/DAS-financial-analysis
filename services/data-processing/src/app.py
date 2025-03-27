from flask import Flask, jsonify
from flask_cors import CORS
import subprocess
import os
import logging
from threading import Thread

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

spark_process = None

def run_spark_streaming():
    global spark_process
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(current_dir, 'spark_streaming.py')
        
        spark_process = subprocess.Popen(
            ['spark-submit', script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        while True:
            output = spark_process.stdout.readline()
            if output:
                logger.info(output.strip())
            error = spark_process.stderr.readline()
            if error:
                logger.error(error.strip())
            
            if spark_process.poll() is not None:
                break
                
    except Exception as e:
        logger.error(f"Error running Spark streaming: {e}")
        raise

@app.route('/api/spark/start', methods=['POST'])
def start_spark_streaming():
    global spark_process
    
    try:
        if spark_process and spark_process.poll() is None:
            return jsonify({
                'status': 'error',
                'message': 'Spark streaming is already running'
            }), 400
            
        thread = Thread(target=run_spark_streaming)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'success',
            'message': 'Spark streaming started successfully'
        })
        
    except Exception as e:
        logger.error(f"Failed to start Spark streaming: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/spark/stop', methods=['POST'])
def stop_spark_streaming():
    global spark_process
    
    try:
        if spark_process and spark_process.poll() is None:
            spark_process.terminate()
            spark_process.wait()
            return jsonify({
                'status': 'success',
                'message': 'Spark streaming stopped successfully'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Spark streaming is not running'
            }), 400
            
    except Exception as e:
        logger.error(f"Failed to stop Spark streaming: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/spark/status', methods=['GET'])
def get_spark_status():
    global spark_process
    
    if spark_process and spark_process.poll() is None:
        return jsonify({
            'status': 'running',
            'message': 'Spark streaming is currently running'
        })
    else:
        return jsonify({
            'status': 'stopped',
            'message': 'Spark streaming is not running'
        })

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
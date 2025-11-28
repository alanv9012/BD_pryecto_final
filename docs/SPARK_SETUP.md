# Spark Setup Guide - No Docker Required!

## Quick Answer: **NO Docker needed!** ✅

This project runs Spark in **local mode** directly on your machine. Here's how to set it up:

## 1. Prerequisites Check

### Check if Java is Installed

**Windows (PowerShell):**
```powershell
java -version
```

**Expected output:**
```
java version "1.8.0_xxx" or higher
```

### Install Java if Missing

**Option A: Download from Oracle**
1. Visit: https://www.oracle.com/java/technologies/downloads/
2. Download Java 8 or higher (Java 11 recommended)
3. Install and add to PATH

**Option B: Use OpenJDK (Recommended)**
- Download from: https://adoptium.net/
- Install the Windows x64 installer
- Java will be automatically added to PATH

### Verify Python is Installed

```powershell
python --version
# Should show Python 3.8 or higher
```

## 2. Install PySpark

Simply install the requirements:

```powershell
pip install -r requirements.txt
```

This installs:
- `pyspark` - The Spark Python API
- `pandas` - For data manipulation
- `pymongo` - For MongoDB
- `jupyter` - For notebooks

**That's it!** PySpark includes Spark and will automatically download/use Spark when you run it.

## 3. How Spark Runs Locally

When you run the notebook:

```python
spark = create_spark_session()
# This creates a Spark session that runs on your machine
# Uses all CPU cores automatically (local[*])
# No Docker, no cluster, just your computer!
```

### What Happens Behind the Scenes:

1. **Spark Driver**: Runs in your Python process
2. **Spark Executors**: Created as threads on your machine
3. **Data Processing**: Uses all available CPU cores
4. **Memory**: Uses your machine's RAM (configured as 4GB)

## 4. Understanding Local Mode

### Current Configuration:

```python
"spark.master": "local[*]"
```

- `local` = Runs on local machine (not a cluster)
- `[*]` = Uses all CPU cores available
- Alternative: `local[4]` = Use only 4 cores

### Local Mode vs Docker:

| Aspect | Local Mode (This Project) | Docker Mode |
|--------|--------------------------|-------------|
| **Setup** | Just install PySpark | Build/run containers |
| **Complexity** | Simple | More complex |
| **Performance** | Uses your machine directly | Overhead from virtualization |
| **Isolation** | Uses your environment | Isolated environment |
| **Best For** | Learning, development | Production, consistency |

## 5. Troubleshooting

### Issue: "Java not found"

**Solution:**
1. Install Java (see above)
2. Add Java to PATH environment variable
3. Restart your terminal/PowerShell
4. Verify with `java -version`

### Issue: "Spark session failed to start"

**Solutions:**
- Check Java installation
- Reduce memory if you have less RAM:
  ```python
  "spark.driver.memory": "2g"  # Instead of 4g
  ```
- Check if another Spark session is running

### Issue: "Out of memory errors"

**Solutions:**
- Reduce Spark memory settings
- Process data in smaller batches
- Increase system RAM if possible

## 6. Optional: Using Docker (If You Want)

If you want to use Docker anyway, you would:

1. Create a Dockerfile:
```dockerfile
FROM python:3.9
RUN apt-get update && apt-get install -y openjdk-11-jdk
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--allow-root"]
```

2. Build and run:
```powershell
docker build -t spark-project .
docker run -p 8888:8888 spark-project
```

**But this is NOT necessary for this project!** ✅

## 7. Why Local Mode is Perfect for This Project

✅ **Simple Setup**: Just install Python packages  
✅ **Fast Development**: No container overhead  
✅ **Easy Debugging**: Direct access to logs  
✅ **Sufficient Power**: Handles large datasets on modern machines  
✅ **No Dependencies**: No Docker, no virtual machines  

## 8. Performance Tips for Local Mode

1. **Close other applications** to free up RAM
2. **Use SSD** for better I/O performance
3. **Adjust memory** based on your RAM:
   ```python
   # If you have 8GB RAM, use:
   "spark.driver.memory": "2g"
   "spark.executor.memory": "2g"
   ```
4. **Process in batches** if data is very large

## 9. Summary

**For this project, you DO NOT need Docker!**

Just:
1. ✅ Install Java
2. ✅ Install Python packages (`pip install -r requirements.txt`)
3. ✅ Run the Jupyter notebook
4. ✅ Spark runs automatically in local mode

**It's that simple!** 🎉

## Need Help?

If you encounter issues:
- Check Java installation: `java -version`
- Check Python version: `python --version`
- Review Spark logs in the notebook output
- Reduce memory settings if needed


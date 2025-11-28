# Quick Start Guide - Step by Step Instructions

This guide will walk you through running the project from scratch on Windows.

## 📋 Step 1: Check Prerequisites

### 1.1 Check if Python is Installed

Open **PowerShell** (press `Win + X` and select "Windows PowerShell" or "Terminal"):

```powershell
python --version
```

**What you should see:**
```
Python 3.8.x or higher
```

**If Python is NOT installed:**
1. Download from: https://www.python.org/downloads/
2. **Important:** Check "Add Python to PATH" during installation
3. Install and restart PowerShell
4. Verify again: `python --version`

### 1.2 Check if Java is Installed

In PowerShell, run:

```powershell
java -version
```

**What you should see:**
```
java version "1.8.0_xxx" or higher
```

**If Java is NOT installed:**
1. Download OpenJDK from: https://adoptium.net/
2. Choose "Windows x64" installer
3. Install (it will add to PATH automatically)
4. **Close and reopen PowerShell**
5. Verify: `java -version`

**Troubleshooting:**
- If `java -version` doesn't work after installing, restart your computer
- Java is required for Spark to run

## 📂 Step 2: Navigate to Project Directory

Open PowerShell and navigate to your project folder:

```powershell
cd "C:\Users\alanv\Documents\Projects\Big data\proyecto_final"
```

Verify you're in the right place:

```powershell
ls
```

You should see folders like `notebooks`, `src`, `data`, and `Taxi Dataset`.

## 📦 Step 3: Create Virtual Environment (Recommended)

This keeps your project dependencies isolated:

```powershell
# Create virtual environment
python -m venv venv

# Activate it
.\venv\Scripts\Activate.ps1
```

**If you get an execution policy error:**
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```
Then try activating again: `.\venv\Scripts\Activate.ps1`

**You'll know it's activated when you see `(venv)` at the start of your prompt.**

## 📥 Step 4: Install Required Packages

With the virtual environment activated, install all dependencies:

```powershell
pip install --upgrade pip
pip install -r requirements.txt
```

**This will install:**
- pyspark (Apache Spark)
- pandas (Data manipulation)
- pymongo (MongoDB connector)
- jupyter (Notebook interface)
- openpyxl (Excel support)

**Wait for installation to complete** - this may take a few minutes.

**Verify installation:**
```powershell
pip list
```
You should see pyspark, pandas, jupyter, etc. in the list.

## 🗂️ Step 5: Verify Your Data Files

Check that your taxi data files are present:

```powershell
ls "Taxi Dataset"
```

You should see files like:
- `yellow_tripdata_2025-01.parquet`
- `yellow_tripdata_2025-02.parquet`
- etc.

**If files are missing:** Make sure you've downloaded the taxi data files to this folder.

## 🚀 Step 6: Start Jupyter Notebook

With your virtual environment still activated:

```powershell
jupyter notebook
```

**What happens:**
- A new browser window/tab opens automatically
- You see the Jupyter file browser
- The URL will be something like: `http://localhost:8888`

**If browser doesn't open automatically:**
- Copy the URL from the terminal (looks like `http://localhost:8888/?token=...`)
- Paste it into your browser

## 📓 Step 7: Open the Analysis Notebook

1. In the Jupyter browser, navigate to the `notebooks` folder
2. Click on `analysis.ipynb` to open it
3. You'll see the notebook with code cells and markdown

## ⚙️ Step 8: Configure MongoDB (Optional for First Run)

**You can skip MongoDB for now** and just run the analysis. The project will:
- ✅ Process all data
- ✅ Generate analysis results
- ✅ Export CSV files for Power BI
- ⚠️ Skip MongoDB storage (until you configure it)

**To configure MongoDB later:**
1. Create account at: https://www.mongodb.com/cloud/atlas/register
2. Create free cluster (M0)
3. Get connection string
4. Update the MongoDB URI in the notebook (cell 2)

## ▶️ Step 9: Run the Notebook

### Option A: Run All Cells at Once

1. In Jupyter, click **Kernel** → **Restart & Run All**
2. Wait for execution (this may take several minutes for large datasets)
3. Watch the output in each cell

### Option B: Run Cells One by One (Recommended for First Time)

1. **Cell 1** (Setup): Click in the cell and press `Shift + Enter`
   - This imports all modules
   - Wait for "Setup complete!" message

2. **Cell 2** (MongoDB Config): Press `Shift + Enter`
   - You'll see a warning about MongoDB (that's OK for now)

3. **Cell 3** (Spark Session): Press `Shift + Enter`
   - **This is the first time Spark starts!**
   - You'll see: "Spark Version: ..."
   - **First run may take 30-60 seconds** to initialize Spark
   - This is normal!

4. **Continue through all cells** one by one
   - Each cell processes a step of the analysis
   - Read the output to understand what's happening

**Shortcuts:**
- `Shift + Enter`: Run cell and move to next
- `Ctrl + Enter`: Run cell but stay in current cell
- `Alt + Enter`: Run cell and create new cell below

## 📊 Step 10: Check Results

### View Results in Notebook

After running all cells, scroll through to see:
- Data schema
- Sample data
- Analysis results
- Summary statistics

### Check Output Files

After the export cell runs, check the `output` folder:

```powershell
# In a new PowerShell window (or close Jupyter and run in terminal)
cd "C:\Users\alanv\Documents\Projects\Big data\proyecto_final"
ls output
```

You should see CSV files:
- `hourly_demand.csv`
- `daily_demand.csv`
- `zone_activity.csv`
- `revenue_analysis.csv`

## 🎉 You're Done!

Your analysis is complete! Next steps:

1. **Review the results** in the notebook
2. **Import CSV files** into Power BI for visualization
3. **Optionally configure MongoDB** for persistent storage

## 🔧 Common Issues & Solutions

### Issue: "Java not found" when starting Spark

**Solution:**
- Make sure Java is installed: `java -version`
- Restart PowerShell after installing Java
- Restart your computer if needed

### Issue: Jupyter notebook won't start

**Solution:**
```powershell
# Reinstall jupyter
pip install --upgrade jupyter

# Or use alternative command
python -m jupyter notebook
```

### Issue: "Out of memory" errors

**Solution:**
Edit `src/config.py` and reduce memory:
```python
"spark.driver.memory": "2g",  # Change from 4g
"spark.executor.memory": "2g"  # Change from 4g
```

### Issue: Notebook kernel dies/restarts

**Solution:**
- Reduce Spark memory settings
- Close other applications to free RAM
- Process fewer files at once (modify code to load specific months)

### Issue: Cells run very slowly

**This is normal for large datasets!**
- Processing 10 months of taxi data takes time
- First run is slower (Spark initialization)
- Subsequent runs are faster (cached)
- Consider processing fewer months for testing

### Issue: Can't see output files

**Solution:**
- Make sure the export cell ran successfully
- Check the `output` folder was created
- Look for error messages in the notebook output

## 📝 Tips for Success

1. **Start small:** Test with 1-2 months of data first
2. **Read output:** Pay attention to cell outputs for progress
3. **Be patient:** Large data processing takes time
4. **Save frequently:** Jupyter auto-saves, but be aware
5. **Check resources:** Monitor RAM/CPU usage in Task Manager

## 🆘 Need More Help?

- Check `docs/SPARK_SETUP.md` for Spark-specific help
- Review `README.md` for project overview
- Check notebook comments for code explanations

## 🎯 Quick Reference Commands

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Start Jupyter
jupyter notebook

# Stop Jupyter
# Press Ctrl+C in the PowerShell window where Jupyter is running

# Deactivate virtual environment
deactivate
```

---

**Ready to start?** Begin with Step 1 and work through each step carefully! 🚀


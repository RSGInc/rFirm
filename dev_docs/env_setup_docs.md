Setting up the rFirm Python Environment
=========================
##Purpose
These instructions are intended to help future users of rFirm set the package up smoothly from GitHub.

##Instructions
1. Ensure that required softwares are installed:
   * A Git Client (https://git-scm.com/)
   * Anaconda Distribution (https://www.anaconda.com/distribution/) for package and environment management. 
   **Important:** You must install the Python 2.7 version of Anaconda Distribution.
2. Go to the rFirm GitHub repository at (https://github.com/RSGInc/rFirm). If you do not have access, contact 
Colin.Smith@rsginc.com. You will not be able to proceed until you have access.
3. Use your local Git Client to clone rFirm onto your local computer. To accomplish this:
   * Navigate within the Git Client to the local folder which you would like to clone the rFirm repository into
   * Run the command ``git clone https://github.com/RSGInc/rFirm.git``
4. Use Anaconda to create an environment for rFirm. To accomplish this open the Anaconda Prompt application and run the 
following commands:
   * Choose a name for the environment (e.g. rFirm_env)
   * Create the new environment ``conda create -n rFirm_env python=2.7``
   * Record the environment location listed in the Anaconda Prompt readout (e.g. C:\Users\...)
   * Activate the environment ``conda activate rFirm_env``
   * Install required Python packages in the environment 
   
     ``conda install cytoolz numpy pandas pyyaml=3.13 psutil``
     
     ``pip install tables orca openmatrix zbox activitysim==0.5.1``
     
   * Deactivate the environment ``conda deactivate``
   * Close the Anaconda Prompt application
5. Within the local version of the rFirm repository, find the script example/run_simulation.py
6. Select the Conda environment you just created (e.g. rFirm_env) to run the example/run_simulation.py script within. 
If you are using the IntelliJ PyCharm IDE for Python, the environment can be selected as follows:
   * Go to File -> Settings to open the settings window
   * On the left sidebar of the **Settings** window, select the *Project: [Project Name]* option
   * Select the *Project Interpreter* option
   * Add an interpreter using the gear symbol near the top-right of the settings window. As you click on the gear symbol,
   select *Add*, and the **Add Python Interpreter** window will open.
   * On the left sidebar select *Conda Environment*
   * Select *Existing Environment*, then place the environment location recorded previously into the interpreter
   location dialog box.
   *Select *OK*
   


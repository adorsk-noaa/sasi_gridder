from sasi_gridder.sasi_gridder_task import SASIGridderTask
from javax.swing import (
    JPanel, JScrollPane, JTextArea, JFrame, JFileChooser, JButton, 
    WindowConstants, JLabel, BoxLayout, JTextField, SpringLayout,
    JProgressBar, SwingConstants
)
from javax.swing.filechooser import FileNameExtensionFilter
from javax.swing.border import EmptyBorder
from java.awt import (Component, BorderLayout, Color)
from java.awt.event import AdjustmentListener
from java.lang import (System, Runtime, Class)
from java.io import File
from java.net import URI
import spring_utilities as SpringUtilities
import os
import csv
import logging
import shutil
from threading import Thread


class FnLogHandler(logging.Handler):
    """ Custom handler to send log messages to a given function. """
    def __init__(self, fn, **kwargs):
        logging.Handler.__init__(self, **kwargs)
        self.fn = fn

    def emit(self, record):
        try:
            self.fn(self.format(record))
        except:
            self.handleError(record)

def browseURI(uri):
    osName = System.getProperty("os.name")
    rt = Runtime.getRuntime()
    if osName.startswith("Mac OS"):
        rt.exec('open "%s"' % uri)
    else:
        if osName.startswith("Windows"):
            rt.exec('rundll32 url.dll,FileProtocolHandler "%s"' % uri)
        else:
            browsers = ["google-chrome", "firefox", "opera", "konqueror", 
                        "epiphany", "mozilla", "netscape" ]
            for b in browsers:
                exists = rt.exec("which %s" % b).getInputStream().read()
                if exists != -1:
                    Runtime.getRuntime().exec('%s %s' % (b, uri))
                    return

class JythonGui(object):
    def __init__(self, instructionsURI=''):
        self.instructionsURI = instructionsURI

        self.logger = logging.getLogger('sasi_gridder_gui')
        self.logger.addHandler(logging.StreamHandler())
        def log_fn(msg):
            self.log_msg(msg)
        self.logger.addHandler(FnLogHandler(log_fn))
        self.logger.setLevel(logging.DEBUG)

        self.selected_input_file = None
        self.selected_output_file = None

        self.frame = JFrame(
            "SASI Gridder",
            defaultCloseOperation = WindowConstants.EXIT_ON_CLOSE,
        )
        self.frame.size = (650, 600,)

        self.main_panel = JPanel()
        self.main_panel.layout = BoxLayout(self.main_panel, BoxLayout.Y_AXIS)
        self.frame.add(self.main_panel)

        self.top_panel = JPanel(SpringLayout())
        self.top_panel.alignmentX = Component.CENTER_ALIGNMENT
        self.main_panel.add(self.top_panel)

        self.stageCounter = 1
        def getStageLabel(txt):
            label = JLabel("%s. %s" % (self.stageCounter, txt))
            self.stageCounter += 1
            return label

        # Instructions link.
        self.top_panel.add(getStageLabel("Read the instructions:"))
        instructionsButton = JButton(
            ('<HTML><FONT color="#000099">'
             '<U>open instructions</U></FONT><HTML>'),
            actionPerformed=self.browseInstructions)
        instructionsButton.setHorizontalAlignment(SwingConstants.LEFT);
        instructionsButton.setBorderPainted(False);
        instructionsButton.setOpaque(False);
        instructionsButton.setBackground(Color.WHITE);
        instructionsButton.setToolTipText(self.instructionsURI);
        self.top_panel.add(instructionsButton)

        # Select input elements.
        self.top_panel.add(getStageLabel("Select an input data folder:"))
        self.top_panel.add(
            JButton("Select input...", actionPerformed=self.openInputChooser))

        # Select output elements.
        self.top_panel.add(getStageLabel("Specify an output file:"))
        self.top_panel.add(
            JButton("Specify output...", actionPerformed=self.openOutputChooser))

        # Run elements.
        self.top_panel.add(getStageLabel(
            "Run SASI Gridder: (this might take a hwile"))
        self.run_button = JButton("Run...", actionPerformed=self.runSASIGridder)
        self.top_panel.add(self.run_button)

        SpringUtilities.makeCompactGrid(
            self.top_panel, self.stageCounter - 1, 2, 6, 6, 6, 6)

        # Progress bar.
        self.progressBar = JProgressBar(0, 100)
        self.main_panel.add(self.progressBar)

        # Log panel.
        self.log_panel = JPanel()
        self.log_panel.alignmentX = Component.CENTER_ALIGNMENT
        self.log_panel.setBorder(EmptyBorder(10,10,10,10))
        self.main_panel.add(self.log_panel)
        self.log_panel.setLayout(BorderLayout())
        self.log = JTextArea()
        self.log.editable = False
        self.logScrollPane = JScrollPane(self.log)
        self.logScrollPane.setVerticalScrollBarPolicy(
            JScrollPane.VERTICAL_SCROLLBAR_ALWAYS)
        self.log_panel.add(self.logScrollPane, BorderLayout.CENTER)

        # File selectors
        self.inputChooser = JFileChooser()
        self.inputChooser.fileSelectionMode = JFileChooser.FILES_AND_DIRECTORIES
        self.outputChooser = JFileChooser()
        self.outputChooser.fileSelectionMode = JFileChooser.FILES_ONLY
        defaultOutputFile = os.path.join(System.getProperty("user.home"),
                                         "gridded_efforts.csv")
        self.outputChooser.setSelectedFile(File(defaultOutputFile));

        self.frame.setLocationRelativeTo(None)
        self.frame.visible = True

    def browseInstructions(self, event):
        """ Open a browser to the instructions page. """
        browseURI(self.instructionsURI)
        return

    def log_msg(self, msg):
        self.log.append(msg + "\n")
        self.log.setCaretPosition(self.log.getDocument().getLength())

    def openInputChooser(self, event):
        ret = self.inputChooser.showOpenDialog(self.frame)
        if ret == JFileChooser.APPROVE_OPTION:
            self.selected_input_file = self.inputChooser.selectedFile
            self.log_msg("Selected '%s' as input." % self.selected_input_file.path)

    def openOutputChooser(self, event):
        ret = self.outputChooser.showSaveDialog(self.frame)
        if ret == JFileChooser.APPROVE_OPTION:
            self.selected_output_file = self.outputChooser.selectedFile
            self.log_msg(
                "Selected '%s' as output." % self.selected_output_file.path)

    def runSASIGridder(self, event):
        try:
            self.validateParameters()
        except Exception as e:
            self.log_msg("ERROR: '%s'" % e)

        # Run task in a separate thread, so that log
        # messages will be shown as task progresses.
        def run_task():

            self.progressBar.setValue(0)
            self.progressBar.setIndeterminate(True)

            try:
                input_dir = self.selected_input_file.path
                output_path = self.selected_output_file.path

                grid_path = os.path.join(
                    input_dir, 'grid', 'grid.shp')

                stat_areas_path = os.path.join(
                    input_dir, 'stat_areas', 'stat_areas.shp')

                raw_efforts_path = os.path.join(
                    input_dir, 'raw_efforts.csv')

                gear_mappings_path = os.path.join(
                    input_dir, 'gear_mappings.csv')

                gear_mappings = {}
                with open(gear_mappings_path, 'rb') as f:
                    r = csv.DictReader(f)
                    for mapping in r:
                        gear_mappings[mapping['trip_type']] = mapping['gear_code']

                task = SASIGridderTask(
                    grid_path=grid_path,
                    raw_efforts_path=raw_efforts_path,
                    stat_areas_path=stat_areas_path,
                    output_path=output_path,
                    logger=self.logger,
                    gear_mappings=gear_mappings,
                    effort_limit=None,
                )
                task.call()
            except Exception as e:
                self.logger.exception("Could not complete task")

            self.progressBar.setIndeterminate(False)
            self.progressBar.setValue(100)

        Thread(target=run_task).start()

    def validateParameters(self):
        return True

def main(*args, **kwargs):
    JythonGui(*args, **kwargs)

if __name__ == '__main__':
    main()

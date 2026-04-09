using System.ComponentModel;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Interop;
using System.Windows.Media;
using System.Windows.Threading;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Hosting;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Infrastructure.Security;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Accounts;
using QadoPoolStack.Desktop.Services.Node;
using QadoPoolStack.Desktop.Services.Tls;
using QadoPoolStack.Desktop.UI;

namespace QadoPoolStack.Desktop;

public partial class App : Application
{
    private const string SingleInstanceMutexName = @"Local\QadoPoolStack.Desktop";
    private const string SingleInstanceActivationEventName = @"Local\QadoPoolStack.Desktop.Activate";

    private PoolLogger? _logger;
    private int _shutdownRequested;
    private int _activateRequested;
    private Mutex? _singleInstanceMutex;
    private EventWaitHandle? _activationEvent;
    private CancellationTokenSource? _activationListenerCts;
    private Task? _activationListenerTask;
    private CancellationTokenSource? _startupCts;
    private Task? _startupTask;

    public DesktopRuntime? Runtime { get; private set; }

    protected override void OnStartup(StartupEventArgs e)
    {
        if (!TryAcquireSingleInstance())
        {
            Shutdown(0);
            return;
        }

        base.OnStartup(e);

        DispatcherUnhandledException += App_DispatcherUnhandledException;
        AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
        TaskScheduler.UnobservedTaskException += TaskScheduler_UnobservedTaskException;
        StartActivationListener();
        ShutdownMode = ShutdownMode.OnMainWindowClose;

        var startupWindow = CreateStartupWindow();
        startupWindow.Closing += StartupWindow_Closing;
        MainWindow = startupWindow;
        startupWindow.Show();
        ActivateMainWindow();

        _startupCts = new CancellationTokenSource();
        _startupTask = InitializeRuntimeAndShowMainWindowAsync(startupWindow, _startupCts);
    }

    protected override void OnExit(ExitEventArgs e)
    {
        CancelStartup();
        StopActivationListener();
        ShutdownRuntimeSync();
        _activationEvent?.Dispose();
        _activationEvent = null;
        _singleInstanceMutex?.ReleaseMutex();
        _singleInstanceMutex?.Dispose();
        _singleInstanceMutex = null;
        base.OnExit(e);
    }

    private void App_DispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
    {
        _logger?.Error("UI", e.Exception, "Unhandled dispatcher exception.");
    }

    private void CurrentDomain_UnhandledException(object? sender, UnhandledExceptionEventArgs args)
    {
        if (args.ExceptionObject is Exception ex)
        {
            _logger?.Error("UI", ex, "Unhandled application exception.");
        }
    }

    private void TaskScheduler_UnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs args)
    {
        _logger?.Error("UI", args.Exception, "Unobserved task exception.");
        args.SetObserved();
    }

    private Window CreateStartupWindow()
    {
        var title = new TextBlock
        {
            Text = "Qado Pool Stack",
            FontSize = 22,
            FontWeight = FontWeights.SemiBold,
            Margin = new Thickness(0, 0, 0, 8)
        };

        var subtitle = new TextBlock
        {
            Text = "Preparing application data and starting the desktop shell...",
            TextWrapping = TextWrapping.Wrap,
            Foreground = Brushes.DimGray,
            Margin = new Thickness(0, 0, 0, 14)
        };

        var progress = new ProgressBar
        {
            IsIndeterminate = true,
            Height = 14
        };

        var panel = new StackPanel
        {
            Margin = new Thickness(24)
        };
        panel.Children.Add(title);
        panel.Children.Add(subtitle);
        panel.Children.Add(progress);

        return new Window
        {
            Title = "Qado Pool Stack",
            Width = 460,
            Height = 180,
            ResizeMode = ResizeMode.NoResize,
            WindowStartupLocation = WindowStartupLocation.CenterScreen,
            Content = panel
        };
    }

    private async Task InitializeRuntimeAndShowMainWindowAsync(Window startupWindow, CancellationTokenSource startupCts)
    {
        var cancellationToken = startupCts.Token;

        try
        {
            var runtime = await CreateRuntimeAsync(cancellationToken).ConfigureAwait(true);
            _logger?.Info("Startup", "Initializing runtime state.");
            await runtime.InitializeAsync(cancellationToken).ConfigureAwait(true);

            if (cancellationToken.IsCancellationRequested || Dispatcher.HasShutdownStarted)
            {
                return;
            }

            _logger?.Info("Startup", "Runtime initialized. Opening main window.");
            Runtime = runtime;

            var mainWindow = new MainWindow(runtime);
            mainWindow.Closing += MainWindow_Closing;
            MainWindow = mainWindow;

            startupWindow.Closing -= StartupWindow_Closing;
            startupWindow.Close();

            mainWindow.Show();
            ActivateMainWindow();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger?.Error("Startup", ex, "Application startup failed.");

            if (cancellationToken.IsCancellationRequested || Dispatcher.HasShutdownStarted)
            {
                return;
            }

            var errorWindow = DialogService.CreateStandalone("Startup failed", ex.ToString());
            MainWindow = errorWindow;

            startupWindow.Closing -= StartupWindow_Closing;
            startupWindow.Close();

            errorWindow.Show();
            ActivateMainWindow();
        }
        finally
        {
            if (ReferenceEquals(_startupCts, startupCts))
            {
                _startupTask = null;
                _startupCts = null;
            }

            startupCts.Dispose();
        }
    }

    private bool TryAcquireSingleInstance()
    {
        _singleInstanceMutex = new Mutex(true, SingleInstanceMutexName, out var createdNew);
        _activationEvent = new EventWaitHandle(false, EventResetMode.AutoReset, SingleInstanceActivationEventName);

        if (createdNew)
        {
            return true;
        }

        try
        {
            _activationEvent.Set();
        }
        catch
        {
            // If the primary instance is still starting up, there is nothing else to do here.
        }

        _singleInstanceMutex.Dispose();
        _singleInstanceMutex = null;
        _activationEvent.Dispose();
        _activationEvent = null;
        return false;
    }

    private void StartActivationListener()
    {
        if (_activationEvent is null)
        {
            return;
        }

        _activationListenerCts = new CancellationTokenSource();
        var stopHandle = _activationListenerCts.Token.WaitHandle;
        var activationHandle = _activationEvent;

        _activationListenerTask = Task.Run(() =>
        {
            WaitHandle[] handles = [activationHandle, stopHandle];

            while (true)
            {
                var signaled = WaitHandle.WaitAny(handles);
                if (signaled == 1)
                {
                    return;
                }

                Interlocked.Exchange(ref _activateRequested, 1);
                _ = Dispatcher.BeginInvoke(ActivateMainWindow);
            }
        });
    }

    private void StopActivationListener()
    {
        if (_activationListenerCts is null)
        {
            return;
        }

        _activationListenerCts.Cancel();
        try
        {
            _activationListenerTask?.Wait(TimeSpan.FromSeconds(2));
        }
        catch
        {
            // Shutdown should continue even if the background wait task is already ending.
        }

        _activationListenerCts.Dispose();
        _activationListenerCts = null;
        _activationListenerTask = null;
    }

    private void StartupWindow_Closing(object? sender, CancelEventArgs args)
    {
        CancelStartup();
    }

    private void ActivateMainWindow()
    {
        var window = MainWindow;
        if (window is null)
        {
            Interlocked.Exchange(ref _activateRequested, 1);
            return;
        }

        if (!window.IsVisible)
        {
            window.Show();
        }

        if (window.WindowState == WindowState.Minimized)
        {
            window.WindowState = WindowState.Normal;
        }

        window.Topmost = true;
        window.Topmost = false;
        window.Activate();
        window.Focus();

        var handle = new WindowInteropHelper(window).Handle;
        if (handle != IntPtr.Zero)
        {
            ShowWindow(handle, SwRestore);
            SetForegroundWindow(handle);
        }

        Interlocked.Exchange(ref _activateRequested, 0);
    }

    private async Task<DesktopRuntime> CreateRuntimeAsync(CancellationToken cancellationToken)
    {
        var paths = new AppPaths();
        var logger = new PoolLogger(paths.LogFilePath);
        var secretProtector = new SecretProtector();
        var settingsStore = new PoolSettingsStore(paths);
        _logger = logger;
        _logger.Info("Startup", $"Preparing runtime in {paths.DataDirectory}");

        var settings = await settingsStore.LoadAsync(cancellationToken).ConfigureAwait(false);
        _logger.Info("Startup", "Settings loaded.");

        var connectionFactory = new SqliteConnectionFactory(paths);
        var repository = new PoolRepository(connectionFactory);
        var acmeChallengeStore = new AcmeChallengeStore();
        var sessionService = new SessionService(repository);
        var passwordHasher = new PasswordHasher();
        var nodeHttpClient = new HttpClient
        {
            BaseAddress = new Uri(settings.NodeBaseUrl.TrimEnd('/')),
            Timeout = TimeSpan.FromSeconds(20)
        };
        var nodeClient = new QadoNodeClient(nodeHttpClient, logger);
        var ledgerService = new LedgerService(repository, nodeClient, secretProtector, settingsStore, logger);
        var letsEncryptService = new LetsEncryptService(paths, secretProtector, acmeChallengeStore, logger);
        var serverHost = new PoolServerHost(paths, repository, secretProtector, logger, acmeChallengeStore);

        _ = new UserAccountService(repository, passwordHasher, secretProtector, sessionService, nodeClient, logger);

        return new DesktopRuntime(paths, settingsStore, repository, serverHost, letsEncryptService, ledgerService, logger, secretProtector);
    }

    private void MainWindow_Closing(object? sender, CancelEventArgs args)
    {
        if (sender is not Window window || Interlocked.CompareExchange(ref _shutdownRequested, 1, 0) != 0)
        {
            return;
        }

        args.Cancel = true;
        window.IsEnabled = false;
        window.Title = "Qado Pool Stack (shutting down)";

        _ = CompleteShutdownAsync(window);
    }

    private async Task CompleteShutdownAsync(Window window)
    {
        try
        {
            if (Runtime is not null)
            {
                await Runtime.ShutdownAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger?.Error("Shutdown", ex, "Application shutdown failed.");
        }

        await Dispatcher.InvokeAsync(() =>
        {
            window.Closing -= MainWindow_Closing;
            window.Close();
        });
    }

    private void ShutdownRuntimeSync()
    {
        try
        {
            Runtime?.ShutdownAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _logger?.Error("Shutdown", ex, "Synchronous runtime shutdown failed.");
        }
    }

    private void CancelStartup()
    {
        if (_startupCts is null)
        {
            return;
        }

        try
        {
            _startupCts.Cancel();
        }
        catch
        {
            // Shutdown should continue even if startup cancellation races with completion.
        }
    }

    private const int SwRestore = 9;

    [DllImport("user32.dll")]
    private static extern bool SetForegroundWindow(IntPtr hWnd);

    [DllImport("user32.dll")]
    private static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
}

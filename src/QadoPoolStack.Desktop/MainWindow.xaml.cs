using System.Collections.ObjectModel;
using System.Globalization;
using System.Windows;
using System.Windows.Media;
using QadoPoolStack.Desktop.Hosting;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.UI;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop;

public partial class MainWindow : Window
{
    private static readonly SolidColorBrush StartServerBackgroundBrush = CreateBrush(0x22, 0x7A, 0x4F);
    private static readonly SolidColorBrush StopServerBackgroundBrush = CreateBrush(0xB0, 0x3A, 0x2E);
    private static readonly SolidColorBrush EnableAccountRegistrationBackgroundBrush = CreateBrush(0x22, 0x7A, 0x4F);
    private static readonly SolidColorBrush DisableAccountRegistrationBackgroundBrush = CreateBrush(0xA9, 0x5A, 0x12);
    private static readonly SolidColorBrush ActionButtonForegroundBrush = CreateBrush(0xFF, 0xFF, 0xFF);
    private readonly DesktopRuntime _runtime;
    private readonly ObservableCollection<string> _logLines = [];
    private bool _loaded;

    public MainWindow(DesktopRuntime runtime)
    {
        _runtime = runtime;
        InitializeComponent();
        LogsListBox.ItemsSource = _logLines;
        _runtime.Logger.EntryWritten += Logger_EntryWritten;
        UpdateServerToggleButtonAppearance(serverRunning: false);
        UpdateAccountRegistrationToggleButtonAppearance(accountRegistrationEnabled: _runtime.Settings.AccountRegistrationEnabled);
    }

    protected override void OnClosed(EventArgs e)
    {
        _runtime.Logger.EntryWritten -= Logger_EntryWritten;
        base.OnClosed(e);
    }

    private async void Window_Loaded(object sender, RoutedEventArgs e)
    {
        if (_loaded)
        {
            return;
        }

        _loaded = true;
        LoadSettingsIntoForm();

        foreach (var entry in _runtime.Logger.RecentEntries)
        {
            _logLines.Add(RenderLog(entry));
        }

        await RefreshAsync().ConfigureAwait(true);
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e)
    {
        await RefreshAsync().ConfigureAwait(true);
    }

    private async void SaveSettingsButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            var settings = ReadSettingsFromForm();
            await _runtime.SaveSettingsAsync(settings).ConfigureAwait(true);
            await RefreshAsync().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            await DialogService.ShowAsync(this, "Save settings", ex.Message).ConfigureAwait(true);
        }
    }

    private async void ServerToggleButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            ServerToggleButton.IsEnabled = false;

            if (_runtime.ServerRunning)
            {
                await _runtime.StopServerAsync().ConfigureAwait(true);
            }
            else
            {
                await _runtime.SaveSettingsAsync(ReadSettingsFromForm()).ConfigureAwait(true);
                await _runtime.StartServerAsync().ConfigureAwait(true);
            }

            await RefreshAsync().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            await DialogService.ShowAsync(this, _runtime.ServerRunning ? "Stop server" : "Start server", ex.Message).ConfigureAwait(true);
        }
        finally
        {
            ServerToggleButton.IsEnabled = true;
        }
    }

    private async void AccountRegistrationToggleButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            AccountRegistrationToggleButton.IsEnabled = false;
            var enabled = !_runtime.Settings.AccountRegistrationEnabled;
            await _runtime.SetAccountRegistrationEnabledAsync(enabled).ConfigureAwait(true);
            await RefreshAsync().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            await DialogService.ShowAsync(this, "Account creation", ex.Message).ConfigureAwait(true);
        }
        finally
        {
            AccountRegistrationToggleButton.IsEnabled = true;
        }
    }

    private async void GeneratePoolKeyButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            var publicKey = await _runtime.GeneratePoolMinerKeyAsync().ConfigureAwait(true);
            PoolPublicKeyTextBox.Text = publicKey;
            PoolPrivateKeyTextBox.Text = _runtime.GetPoolMinerPrivateKey() ?? string.Empty;
            await DialogService.ShowAsync(this, "Pool key", "A new pool mining keypair was generated and stored locally.").ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            await DialogService.ShowAsync(this, "Pool key", ex.Message).ConfigureAwait(true);
        }
    }

    private async void AcceptPoolKeyButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            var publicKey = await _runtime.SetPoolMinerPrivateKeyAsync(PoolPrivateKeyTextBox.Text ?? string.Empty).ConfigureAwait(true);
            PoolPublicKeyTextBox.Text = publicKey;
            PoolPrivateKeyTextBox.Text = _runtime.GetPoolMinerPrivateKey() ?? string.Empty;
            await RefreshAsync().ConfigureAwait(true);
            await DialogService.ShowAsync(this, "Pool key", "The pool private key was accepted and saved.").ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            await DialogService.ShowAsync(this, "Pool key", ex.Message).ConfigureAwait(true);
        }
    }

    private async void DeletePoolKeyButton_Click(object sender, RoutedEventArgs e)
    {
        if (MessageBox.Show(
                this,
                "Delete the configured pool keypair? This will clear the pool address until a new private key is accepted or generated.",
                "Delete pool key",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning) != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            await _runtime.ClearPoolMinerKeyAsync().ConfigureAwait(true);
            LoadSettingsIntoForm();
            await RefreshAsync().ConfigureAwait(true);
            await DialogService.ShowAsync(this, "Pool key", "The pool keypair was deleted.").ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            await DialogService.ShowAsync(this, "Pool key", ex.Message).ConfigureAwait(true);
        }
    }

    private async void AcquireTlsButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            await _runtime.SaveSettingsAsync(ReadSettingsFromForm()).ConfigureAwait(true);
            await _runtime.AcquireTlsCertificateAsync().ConfigureAwait(true);
            LoadSettingsIntoForm();
            var message = _runtime.Settings.UseLetsEncryptStaging
                ? "Staging certificate acquired and saved. Browsers and operating systems will mark this certificate as untrusted because it is for testing only. Uncheck Let's Encrypt staging and acquire a new certificate for public HTTPS use. Restart the server to bind HTTPS with the new certificate."
                : "Certificate acquired and saved. Restart the server to bind HTTPS with the new certificate.";
            await DialogService.ShowAsync(this, "TLS", message).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            await DialogService.ShowAsync(this, "TLS", ex.Message).ConfigureAwait(true);
        }
    }

    private async void WithdrawAllUserButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement { DataContext: UserRow userRow })
        {
            return;
        }

        if (!userRow.CanWithdraw || userRow.AvailableAtomic <= 0)
        {
            await DialogService.ShowAsync(this, "Withdraw user", "This user has no available balance to withdraw.").ConfigureAwait(true);
            return;
        }

        var addressText = string.IsNullOrWhiteSpace(userRow.WithdrawalAddress) ? "no verified withdrawal address" : userRow.WithdrawalAddress;
        var confirmation = MessageBox.Show(
            this,
            $"Withdraw the full available balance of {userRow.Available} QADO for user '{userRow.Username}' to {addressText}?\n\nThis will immediately create and broadcast the withdrawal with zero fee.",
            "Confirm full withdrawal",
            MessageBoxButton.YesNo,
            MessageBoxImage.Warning);

        if (confirmation != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            var request = await _runtime.WithdrawAllForUserAsync(userRow.UserId).ConfigureAwait(true);
            await RefreshAsync().ConfigureAwait(true);
            await DialogService.ShowAsync(
                    this,
                    "Withdraw user",
                    $"Withdrew {userRow.Available} QADO for user '{userRow.Username}'.\n\nRequest id: {request.WithdrawalId}\nTx id: {request.ExternalTxId ?? "-"}")
                .ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            await DialogService.ShowAsync(this, "Withdraw user", ex.Message).ConfigureAwait(true);
        }
    }

    private void Logger_EntryWritten(PoolLogEntry entry)
    {
        _ = Dispatcher.InvokeAsync(() =>
        {
            var line = RenderLog(entry);
            _logLines.Add(line);

            while (_logLines.Count > 300)
            {
                _logLines.RemoveAt(0);
            }

            LogsListBox.SelectedItem = line;
            LogsListBox.ScrollIntoView(line);
        });
    }

    private async Task RefreshAsync()
    {
        var dashboardTask = _runtime.GetDashboardAsync();
        var usersTask = _runtime.ListUsersAsync();
        var withdrawalsTask = _runtime.ListWithdrawalsAsync();
        var roundsTask = _runtime.ListRecentRoundsAsync();
        var blocksTask = _runtime.ListRecentBlocksAsync();

        await Task.WhenAll(dashboardTask, usersTask, withdrawalsTask, roundsTask, blocksTask).ConfigureAwait(true);

        var dashboard = dashboardTask.Result;
        var currentAccessSummary = _runtime.GetServerAccessSummary();
        var pendingAccessSummary = _runtime.GetPendingServerAccessSummary();
        SummaryServerTextBlock.Text = dashboard.ServerRunning
            ? $"Running{Environment.NewLine}{currentAccessSummary}"
            : $"Stopped{Environment.NewLine}{currentAccessSummary}";
        SummaryRoundTextBlock.Text = dashboard.CurrentRoundHeight;
        SummaryTrackedBalanceTextBlock.Text = AmountUtility.FormatAtomic(ParseLong(dashboard.TotalTrackedBalanceAtomic));
        SummaryWithdrawalCountTextBlock.Text = dashboard.PendingWithdrawalCount.ToString(CultureInfo.InvariantCulture);
        SummaryUsersTextBlock.Text = dashboard.UserCount.ToString(CultureInfo.InvariantCulture);
        SummaryMinersTextBlock.Text = dashboard.VerifiedMinerCount.ToString(CultureInfo.InvariantCulture);
        SummarySharesTextBlock.Text = dashboard.OpenRoundShareCount.ToString(CultureInfo.InvariantCulture);
        SummaryPrevHashTextBlock.Text = string.IsNullOrWhiteSpace(dashboard.CurrentRoundPrevHash) ? "-" : dashboard.CurrentRoundPrevHash;
        UpdateServerToggleButtonAppearance(dashboard.ServerRunning);
        UpdateAccountRegistrationToggleButtonAppearance(_runtime.Settings.AccountRegistrationEnabled);
        ServerStatusTextBlock.Text = dashboard.ServerRunning
            ? _runtime.HasPendingServerSettingsChanges()
                ? $"Server online. {currentAccessSummary} Restart required to apply saved settings." +
                  (string.IsNullOrWhiteSpace(pendingAccessSummary) ? string.Empty : $" Next start: {pendingAccessSummary}.")
                : $"Server online. {currentAccessSummary}"
            : $"Server offline. {currentAccessSummary} Saved settings will apply on next start.";

        UsersDataGrid.ItemsSource = usersTask.Result.Select(user => new UserRow(
            user.UserId,
            user.Username,
            user.AvailableAtomic,
            AmountUtility.FormatAtomic(user.AvailableAtomic),
            AmountUtility.FormatAtomic(user.PendingWithdrawalAtomic),
            AmountUtility.FormatAtomic(user.TotalDepositedAtomic),
            AmountUtility.FormatAtomic(user.TotalMinedAtomic),
            AmountUtility.FormatAtomic(user.TotalWithdrawnAtomic),
            user.WithdrawalAddressHex,
            user.AvailableAtomic > 0 && !string.IsNullOrWhiteSpace(user.WithdrawalAddressHex))).ToList();

        var userLookup = usersTask.Result.ToDictionary(x => x.UserId, x => x.Username, StringComparer.Ordinal);
        WithdrawalsDataGrid.ItemsSource = withdrawalsTask.Result.Select(item => new WithdrawalRow(
            item.WithdrawalId,
            userLookup.TryGetValue(item.UserId, out var username) ? username : item.UserId,
            AmountUtility.FormatAtomic(item.AmountAtomic),
            item.DestinationAddressHex,
            string.IsNullOrWhiteSpace(item.ExternalTxId) ? "-" : item.ExternalTxId,
            item.Status.ToString(),
            item.RequestedUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture))).ToList();

        RoundsDataGrid.ItemsSource = roundsTask.Result.Select(round => new RoundRow(
            round.HeightText,
            round.PrevHashHex,
            round.Status.ToString(),
            round.OpenedUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            round.ClosedUtc?.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) ?? "-")).ToList();

        BlocksDataGrid.ItemsSource = blocksTask.Result.Select(block => new BlockRow(
            block.HeightText,
            AmountUtility.FormatAtomic(ParseLong(block.RewardAtomicText)),
            block.BlockHashHex,
            block.AcceptedUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture))).ToList();
    }

    private void LoadSettingsIntoForm()
    {
        var settings = _runtime.Settings;
        NodeUrlTextBox.Text = settings.NodeBaseUrl;
        HttpPortTextBox.Text = settings.HttpPort.ToString(CultureInfo.InvariantCulture);
        HttpsPortTextBox.Text = settings.HttpsPort.ToString(CultureInfo.InvariantCulture);
        EnableHttpsCheckBox.IsChecked = settings.EnableHttps;
        PreferHttpsWhenAvailableCheckBox.IsChecked = settings.PreferHttpsWhenAvailable;
        DomainTextBox.Text = settings.DomainName;
        LetsEncryptEmailTextBox.Text = settings.LetsEncryptEmail;
        LetsEncryptStagingCheckBox.IsChecked = settings.UseLetsEncryptStaging;
        CertificatePathTextBox.Text = settings.CertificatePath;
        PoolPublicKeyTextBox.Text = settings.PoolMinerPublicKey;
        PoolPrivateKeyTextBox.Text = _runtime.GetPoolMinerPrivateKey() ?? string.Empty;
        DefaultDifficultyTextBox.Text = DifficultyCalibration.ToCalibratedDifficulty(settings.DefaultShareDifficulty, settings).ToString("0.00", CultureInfo.InvariantCulture);
        ShareRateLimitTextBox.Text = settings.ShareRateLimitPerMinute.ToString(CultureInfo.InvariantCulture);
        PoolFeeTextBox.Text = settings.PoolFeeBasisPoints.ToString(CultureInfo.InvariantCulture);
    }

    private Configuration.PoolSettings ReadSettingsFromForm()
    {
        var settings = _runtime.Settings.Clone();
        settings.NodeBaseUrl = (NodeUrlTextBox.Text ?? string.Empty).Trim();
        settings.HttpPort = ParseInt(HttpPortTextBox.Text, "HTTP port");
        settings.HttpsPort = ParseInt(HttpsPortTextBox.Text, "HTTPS port");
        settings.EnableHttps = EnableHttpsCheckBox.IsChecked == true;
        settings.PreferHttpsWhenAvailable = PreferHttpsWhenAvailableCheckBox.IsChecked == true;
        settings.DomainName = (DomainTextBox.Text ?? string.Empty).Trim();
        settings.LetsEncryptEmail = (LetsEncryptEmailTextBox.Text ?? string.Empty).Trim();
        settings.UseLetsEncryptStaging = LetsEncryptStagingCheckBox.IsChecked == true;
        settings.CertificatePath = (CertificatePathTextBox.Text ?? string.Empty).Trim();
        settings.DefaultShareDifficulty = DifficultyCalibration.ToRawDifficulty(ParseDouble(DefaultDifficultyTextBox.Text, "Default share difficulty"), settings);
        settings.ShareRateLimitPerMinute = ParseInt(ShareRateLimitTextBox.Text, "Share rate limit");
        settings.PoolFeeBasisPoints = ParseInt(PoolFeeTextBox.Text, "Pool fee");
        return settings;
    }

    private static int ParseInt(string? text, string fieldName)
    {
        if (!int.TryParse((text ?? string.Empty).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
        {
            throw new InvalidOperationException($"{fieldName} must be an integer.");
        }

        return value;
    }

    private static double ParseDouble(string? text, string fieldName)
    {
        if (!double.TryParse((text ?? string.Empty).Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
        {
            throw new InvalidOperationException($"{fieldName} must be a number.");
        }

        return value;
    }

    private static long ParseLong(string? text, string fieldName = "Value")
    {
        if (!long.TryParse((text ?? string.Empty).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
        {
            throw new InvalidOperationException($"{fieldName} must be an integer.");
        }

        return value;
    }

    private static string RenderLog(PoolLogEntry entry)
        => $"{entry.TimestampUtc:HH:mm:ss} [{entry.Level}] {entry.Category} {entry.Message}";

    private void UpdateServerToggleButtonAppearance(bool serverRunning)
    {
        ServerToggleButton.Content = serverRunning ? "Stop Server" : "Start Server";
        ServerToggleButton.Background = serverRunning ? StopServerBackgroundBrush : StartServerBackgroundBrush;
        ServerToggleButton.BorderBrush = serverRunning ? StopServerBackgroundBrush : StartServerBackgroundBrush;
        ServerToggleButton.Foreground = ActionButtonForegroundBrush;
    }

    private void UpdateAccountRegistrationToggleButtonAppearance(bool accountRegistrationEnabled)
    {
        AccountRegistrationToggleButton.Content = accountRegistrationEnabled ? "Disable Account Creation" : "Enable Account Creation";
        AccountRegistrationToggleButton.Background = accountRegistrationEnabled ? DisableAccountRegistrationBackgroundBrush : EnableAccountRegistrationBackgroundBrush;
        AccountRegistrationToggleButton.BorderBrush = accountRegistrationEnabled ? DisableAccountRegistrationBackgroundBrush : EnableAccountRegistrationBackgroundBrush;
        AccountRegistrationToggleButton.Foreground = ActionButtonForegroundBrush;
    }

    private static SolidColorBrush CreateBrush(byte red, byte green, byte blue)
    {
        var brush = new SolidColorBrush(Color.FromRgb(red, green, blue));
        brush.Freeze();
        return brush;
    }

    private sealed record UserRow(
        string UserId,
        string Username,
        long AvailableAtomic,
        string Available,
        string Pending,
        string Deposited,
        string Mined,
        string Withdrawn,
        string? WithdrawalAddress,
        bool CanWithdraw);

    private sealed record WithdrawalRow(
        string WithdrawalId,
        string Username,
        string Amount,
        string Address,
        string TxId,
        string Status,
        string RequestedUtc);

    private sealed record RoundRow(
        string Height,
        string PrevHash,
        string Status,
        string OpenedUtc,
        string ClosedUtc);

    private sealed record BlockRow(
        string Height,
        string Reward,
        string Hash,
        string AcceptedUtc);
}

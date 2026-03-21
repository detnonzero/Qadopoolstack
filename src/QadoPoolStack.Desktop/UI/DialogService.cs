using System.Windows;
using System.Windows.Controls;

namespace QadoPoolStack.Desktop.UI;

public static class DialogService
{
    public static async Task ShowAsync(Window? owner, string title, string message)
    {
        var dialog = CreateMessageWindow(title, message);

        if (owner is not null)
        {
            dialog.Owner = owner;
            dialog.WindowStartupLocation = WindowStartupLocation.CenterOwner;
            dialog.ShowDialog();
        }
        else
        {
            dialog.WindowStartupLocation = WindowStartupLocation.CenterScreen;
            dialog.ShowDialog();
        }

        await Task.CompletedTask;
    }

    public static Window CreateStandalone(string title, string message)
    {
        var window = CreateMessageWindow(title, message);
        window.WindowStartupLocation = WindowStartupLocation.CenterScreen;
        return window;
    }

    private static Window CreateMessageWindow(string title, string message)
    {
        var window = new Window
        {
            Title = title,
            Width = 680,
            Height = 420,
            MinWidth = 520,
            MinHeight = 280,
            ResizeMode = ResizeMode.CanResize,
            Content = new Grid
            {
                Margin = new Thickness(16),
                RowDefinitions =
                {
                    new RowDefinition { Height = GridLength.Auto },
                    new RowDefinition(),
                    new RowDefinition { Height = GridLength.Auto }
                }
            }
        };

        var rootGrid = (Grid)window.Content;

        rootGrid.Children.Add(new TextBlock
        {
            Text = title,
            FontSize = 20,
            FontWeight = FontWeights.Bold,
            Margin = new Thickness(0, 0, 0, 12)
        });

        var messageTextBox = new TextBox
        {
            Text = message,
            IsReadOnly = true,
            AcceptsReturn = true,
            TextWrapping = TextWrapping.Wrap,
            VerticalScrollBarVisibility = ScrollBarVisibility.Auto,
            HorizontalScrollBarVisibility = ScrollBarVisibility.Auto
        };
        Grid.SetRow(messageTextBox, 1);
        rootGrid.Children.Add(messageTextBox);

        var closeButton = new Button
        {
            Content = "OK",
            Width = 100,
            IsDefault = true,
            Margin = new Thickness(0, 12, 0, 0),
            HorizontalAlignment = HorizontalAlignment.Right
        };
        closeButton.Click += (_, _) => window.Close();
        Grid.SetRow(closeButton, 2);
        rootGrid.Children.Add(closeButton);

        return window;
    }
}

require(shiny)
require(shinydashboard)
require(dplyr)
require(plotly)
require(DT)
require(lubridate)

ui <- dashboardPage(
  dashboardHeader(title = "reddit dashboard"),
  dashboardSidebar(
    width = 200,
    menuItem("Reddit", tabName = "panel1", icon = icon("dashboard"))
  ),
  dashboardBody(
    tags$style(HTML("

      .main-header .logo {
          background-color: red !important;
        }

      .main-header .navbar {
          background-color: red !important;
        }

      .content-wrapper {
        background-color: white !important;
      }

      .fluid-row-pad {
      padding-bottom: 20px;
      }

      .index-text {
        font-size: 20px;
        }

      .bootstrap-select .dropdown-menu .selected a {
        background-color: #A52A2A !important;
        color: white !important;
      }
      .bootstrap-select .btn.dropdown-toggle.btn-primary {
        background-color: #A52A2A !important;
      }
      .btn-group .dropdown-menu {
        background-color: #A52A2A !important;
      }

    ")),
    tabsetPanel(
      id = "tabset1",

      # UI-overview -------------------------------------------------------------

      tabPanel(
        "Overview",
        #UI of key metrics
        fluidRow(
          class = "fluid-row-pad",
          column(3, div("No of Subreddit: ", style = "font-weight: bold;font-size: 20px;"), div(textOutput("no_sr"), class = "index-text")),
          column(3, div("No of Submission: ", style = "font-weight: bold;font-size: 20px;"), div(textOutput("no_sm"), class = "index-text")),
          column(3, div("No of Comment: ", style = "font-weight: bold;font-size: 20px;"), div(textOutput("no_cm"), class = "index-text")),
          column(3, div("The latest data are up to:", style = "font-weight: bold;font-size: 20px;"), div(textOutput("txt_date"), class = "index-text"))
        ),
        #UI of line charts
        fluidRow(
          column(6, plotlyOutput("line_CM_All")),
          column(6, plotlyOutput("line_CM_R"))
        ),
        #UI of stacked bar chart
        fluidRow(
          column(12, plotlyOutput("stacked_User"))
        )
      ),

      # UI-user -----------------------------------------------------------------

      tabPanel(
        "User",
        fluidRow(
          class = "col-sm-12",
          #UI of dropdown and slider
          column(2, htmlOutput("dd_sr")),
          column(2, htmlOutput("dd_ar")),
          column(6, uiOutput("date_slider"))
        ),
        #UI of 2 Bar charts
        fluidRow(
          column(6, plotlyOutput("bar_CM_author_dt", height = "350px")),
          column(6, plotlyOutput("bar_CM_author_hr", height = "350px"))
        ),
        #UI of Table
        fluidRow(column(12, DT::dataTableOutput("authorTable")))
      )
    )
  )
)


server <- function(input, output, session) {
  # server-overview ---------------------------------------------------------

  # Read raw data
  dfRaw <- readRDS("/srv/shiny-server/reddit_dashboard/labeled_data_final.rds")

  dfRaw$date_local <- as.Date(dfRaw$date_local)
  dfRaw$hour_local <- as.numeric(dfRaw$hour_local)
  latest_date <- as.character(max(dfRaw$date_local))

  output$txt_date <- renderText({
    latest_date
  })

  # #Number of subreddit
  no_subreddit <- dfRaw %>%
    distinct(sub_reddit) %>%
    nrow()
  output$no_sr <- renderText({
    no_subreddit
  })

  # #Number of submission
  no_submission <- dfRaw %>%
    distinct(submission_id) %>%
    nrow()
  output$no_sm <- renderText({
    format(no_submission, big.mark = ",", scientific = FALSE)
  })

  # #Number of submission
  no_comment <- dfRaw %>%
    distinct(comment_id) %>%
    nrow()
  output$no_cm <- renderText({
    format(no_comment, big.mark = ",", scientific = FALSE)
  })

  # Line chart of all comment, group by subreddit
  dfCM_all <- dfRaw %>%
    group_by(date_local, sub_reddit) %>%
    summarise(
      no_comment = n()
    ) %>%
    ungroup()

  output$line_CM_All <- renderPlotly({
    ggplotly(
      ggplot(dfCM_all, aes(x = date_local, y = no_comment, color = sub_reddit)) +
        geom_line() +
        geom_point() +
        theme_minimal() +
        ggtitle("<b>Total Comment by Subreddit</b>") +
        theme(
          panel.background = element_blank(),
          panel.grid.major = element_blank(),
          panel.grid.minor = element_blank(),
          axis.line = element_line(colour = "black"),
          plot.title = element_text(hjust = 0.5)
        )
    )
  })

  # Line chart of radical comment, group by subreddit
  dfCM_r <- dfRaw %>%
    filter(prediction == 1) %>%
    group_by(date_local, sub_reddit) %>%
    summarise(
      no_comment = n()
    ) %>%
    ungroup()

  output$line_CM_R <- renderPlotly({
    ggplotly(
      ggplot(dfCM_r, aes(x = date_local, y = no_comment, color = sub_reddit)) +
        geom_line() +
        geom_point() +
        theme_minimal() +
        ggtitle("<b>Total Radical Comment by Subreddit</b>") +
        theme(
          panel.background = element_blank(),
          panel.grid.major = element_blank(),
          panel.grid.minor = element_blank(),
          axis.line = element_line(colour = "black"),
          plot.title = element_text(hjust = 0.5)
        )
    )
  })

  # Stacked bar chart of top 15 users who make radical comment
  dfUser <- dfRaw %>%
    group_by(author, prediction) %>%
    summarise(
      no_comment = n()
    ) %>%
    ungroup()

  extreme_authors <- dfUser %>%
    filter(prediction == 1) %>%
    group_by(author) %>%
    summarise(total_extreme_comments = sum(no_comment)) %>%
    arrange(desc(total_extreme_comments)) %>%
    top_n(10, total_extreme_comments)

  top_authors <- extreme_authors$author

  top_authors_data <- dfUser %>%
    filter(author %in% top_authors)

  top_authors_data <- top_authors_data %>%
    mutate(author = factor(author, levels = top_authors))

  output$stacked_User <- renderPlotly({
    p <- plot_ly(top_authors_data,
      x = ~author, y = ~no_comment,
      type = "bar", color = ~ factor(prediction), colors = c("lightgreen", "#f9bfbf")
    ) %>%
      layout(
        title = "<b>Top 15 Users Who Make Radical Comment</b>",
        yaxis = list(title = "Number of Comments"),
        xaxis = list(title = "Author"),
        barmode = "stack"
      )
    p
  })

  
  # server-user -------------------------------------------------------------

  drop_down <- reactiveValues()

  drop_down$dfRaw_author <- dfRaw %>%
    filter(prediction == 1) %>%
    group_by(sub_reddit, author) %>%
    summarise(no_comment = n(), .groups = "drop") %>%
    arrange(sub_reddit, desc(no_comment)) %>%
    group_by(sub_reddit) %>%
    slice_head(n = 5) %>%
    arrange(desc(no_comment)) %>%
    ungroup()

  output$dd_sr <- renderUI({
    list_sr <- unique(drop_down$dfRaw_author$sub_reddit) %>% na.omit()

    selectInput("id_sr", "Choose an Subreddit:",
      choices = list_sr, selected = list_sr[1]
    )
  })

  observeEvent(input$id_sr, {
    drop_down$df_ar <- drop_down$dfRaw_author %>%
      filter(sub_reddit == input$id_sr) %>%
      select(author)
  })


  output$dd_ar <- renderUI({
    list_ar <- unique(drop_down$df_ar$author) %>% na.omit()

    selectInput("id_ar", "Top 5 Radical Author:",
      choices = list_ar, selected = list_ar[1]
    )
  })

  observeEvent(input$id_ar, {
    drop_down$df_author <- dfRaw %>% filter(author == input$id_ar)
  })

  output$date_slider <- renderUI({
    min_date <- min(drop_down$df_author$date_local, na.rm = TRUE)
    max_date <- max(drop_down$df_author$date_local, na.rm = TRUE)
    req(min_date, max_date)

    sliderInput("dateRange_days",
      "Date Range:",
      min = min_date,
      max = max_date,
      value = c(min_date, max_date),
      timeFormat = "%Y-%m-%d", step = 1
    )
  })

  observeEvent(c(input$dateRange_days[1], input$dateRange_days[2]), {
    req(input$dateRange_days[1], input$dateRange_days[2])

    drop_down$df_author_selected <- drop_down$df_author %>% filter(date_local >= input$dateRange_days[1] & date_local <= input$dateRange_days[2] & prediction == 1)

    drop_down$df_author_selected_dt <- drop_down$df_author_selected %>%
      group_by(date_local) %>%
      summarise(
        no_comment = n()
      ) %>%
      ungroup()
    
    #Bar chart by date
    output$bar_CM_author_dt <- renderPlotly({
      plot <- ggplot(drop_down$df_author_selected_dt, aes(x = date_local, y = no_comment)) +
        geom_col(width = 0.5) +
        theme_minimal() +
        ggtitle("<b>Total Radical Comment by Day</b>") +
        theme(
          panel.background = element_blank(),
          panel.grid.major = element_blank(),
          panel.grid.minor = element_blank(),
          axis.line = element_line(colour = "black"),
          plot.title = element_text(hjust = 0.5),
          axis.text.x = element_text(angle = 0, hjust = 0.5)
        )
      ggplotly(plot)
    })

    drop_down$df_author_selected_hr <- drop_down$df_author_selected %>%
      group_by(hour_local) %>%
      summarise(
        no_comment = n()
      ) %>%
      ungroup()

    #Bar chart by hour
    output$bar_CM_author_hr <- renderPlotly({
      plot <- ggplot(drop_down$df_author_selected_hr, aes(x = hour_local, y = no_comment)) +
        geom_col(width = 0.5) +
        theme_minimal() +
        ggtitle("<b>Total Radical Comment by Hour</b>") +
        theme(
          panel.background = element_blank(),
          panel.grid.major = element_blank(),
          panel.grid.minor = element_blank(),
          axis.line = element_line(colour = "black"),
          plot.title = element_text(hjust = 0.5),
          axis.text.x = element_text(angle = 0, hjust = 0.5)
        ) +
        scale_x_continuous(breaks = 0:23, limits = c(-0.5, 23.5))

      ggplotly(plot)
    })
  })

  # Comment Table
  output$authorTable <- DT::renderDT({
    DT::datatable(
      drop_down$df_author_selected,
      options = list(pageLength = 5)
    )
  })
}

shinyApp(ui = ui, server = server)